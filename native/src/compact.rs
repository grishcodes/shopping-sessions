use crate::{
    CopyOfEvent,
    input::{BAD, Key},
    singletons::location,
    tables::RawEvent,
};
use crate::fastmap::U64Map;

// Two distinct markers leave offsets 0..=4_294_967_293 available.
const INVALID: u32 = u32::MAX - 1;
const WIDE: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct Packed {
    id: Key,
    user_or_slot: u64,
    event_ms: u32,
    ingest_ms: i32,
}
#[derive(Clone, Copy)]
struct Extended {
    event: RawEvent,
    ingest: i64,
}
pub(crate) struct Part {
    rows: Vec<Packed>,
    overflow: Vec<Extended>,
}

const _: () = assert!(size_of::<Packed>() == 24);
const _: () = assert!(size_of::<Extended>() == 32);

impl Part {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            rows: Vec::with_capacity(capacity),
            overflow: Vec::new(),
        }
    }
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    #[inline(always)]
    pub(crate) fn push(&mut self, id: Key, ingest: i64, event: RawEvent, origin: i64) {
        let ingest_delta = ingest.wrapping_sub(origin);
        let event_delta = event.ts.wrapping_sub(origin) as u64;
        let row = if ingest_delta == i64::from(ingest_delta as i32)
            && (event.ts == BAD || event_delta < u64::from(INVALID))
            && event.price == 0
            && event.category == 0
        {
            Packed {
                id,
                user_or_slot: event.user,
                event_ms: if event.ts == BAD {
                    INVALID
                } else {
                    event_delta as u32
                },
                ingest_ms: ingest_delta as i32,
            }
        } else {
            let slot = self.overflow.len();
            self.overflow.push(Extended { event, ingest });
            Packed {
                id,
                user_or_slot: slot as u64,
                event_ms: WIDE,
                ingest_ms: 0,
            }
        };
        self.rows.push(row);
    }

    #[inline(always)]
    fn ingest(&self, row: &Packed, origin: i64) -> i64 {
        if row.event_ms == WIDE {
            self.overflow[row.user_or_slot as usize].ingest
        } else {
            origin.wrapping_add(i64::from(row.ingest_ms))
        }
    }

    #[inline(always)]
    fn event(&self, row: &Packed, origin: i64) -> RawEvent {
        if row.event_ms == WIDE {
            return self.overflow[row.user_or_slot as usize].event;
        }
        let ts = if row.event_ms == INVALID {
            BAD
        } else {
            origin.wrapping_add(i64::from(row.event_ms))
        };
        RawEvent {
            user: row.user_or_slot,
            ts,
            price: 0,
            category: 0,
        }
    }

    pub(crate) fn expand(&self, origin: i64) -> Vec<CopyOfEvent> {
        self.rows
            .iter()
            .map(|row| CopyOfEvent {
                id: row.id,
                ingest: self.ingest(row, origin),
                event: self.event(row, origin),
            })
            .collect()
    }
}

pub(crate) fn buckets(estimated: usize) -> Vec<Part> {
    (0..crate::n_shards())
        .map(|_| Part::with_capacity(estimated.div_ceil(crate::n_shards())))
        .collect()
}

pub(crate) fn transpose(blocks: Vec<Vec<Part>>) -> Vec<Vec<Part>> {
    let mut shards: Vec<Vec<Part>> = (0..crate::n_shards()).map(|_| Vec::new()).collect();
    for block in blocks {
        for (shard, part) in shards.iter_mut().zip(block) {
            if part.len() > 0 {
                shard.push(part);
            }
        }
    }
    shards
}

pub(crate) fn report(blocks: &[Vec<Part>]) {
    let mut rows = 0;
    let mut extended = 0;
    let mut capacity = 0;
    for part in blocks.iter().flatten() {
        rows += part.len();
        extended += part.overflow.len();
        capacity += part.rows.capacity() * size_of::<Packed>()
            + part.overflow.capacity() * size_of::<Extended>();
    }
    eprintln!(
        "event copies primary_rows={rows} extended_rows={extended} live_bytes={} capacity_bytes={capacity}",
        rows * size_of::<Packed>() + extended * size_of::<Extended>()
    );
}

#[derive(Clone, Copy, Default)]
struct Bits {
    seen: u64,
    repeated: u64,
}

/// Same two-bit singleton filter and exact survivor selection as the full layout.
/// A reference to its fragment travels with an ambiguous row to resolve its
/// optional extended slot. Singleton rows never materialize an ingest timestamp.
pub(crate) fn select(parts: &[Part], origin: i64, mut emit: impl FnMut(RawEvent)) {
    let count: usize = parts.iter().map(Part::len).sum();
    if count == 0 {
        return;
    }
    let words = count.div_ceil(8).next_power_of_two();
    let mask = words - 1;
    let mut bitmap = vec![Bits::default(); words];
    for part in parts {
        for row in &part.rows {
            let (i, b) = location(row.id, mask);
            let w = &mut bitmap[i];
            w.repeated |= w.seen & b;
            w.seen |= b;
        }
    }
    // Ambiguous IDs (repeated, or sharing filter bits with a repeated ID)
    // resolve exactly: the earliest ingest wins, keyed by the full ID.
    // Values are (part index, row index) into `parts`.
    let mut ambiguous = U64Map::<(u32, u32)>::with_capacity(count / 16, (0, 0));
    for (pi, part) in parts.iter().enumerate() {
        for (ri, row) in part.rows.iter().enumerate() {
            let (i, b) = location(row.id, mask);
            if bitmap[i].repeated & b != b {
                emit(part.event(row, origin));
            } else {
                let here = (pi as u32, ri as u32);
                let ((opi, ori), fresh) = ambiguous.entry(row.id, || here);
                if !fresh {
                    let previous = &parts[opi as usize];
                    let old = &previous.rows[ori as usize];
                    if part.ingest(row, origin) < previous.ingest(old, origin) {
                        ambiguous.set(row.id, here);
                    }
                }
            }
        }
    }
    for (_, (pi, ri)) in ambiguous.iter() {
        let part = &parts[pi as usize];
        emit(part.event(&part.rows[ri as usize], origin));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event(user: u64, ts: i64, price: u32, category: u32) -> RawEvent {
        RawEvent {
            user,
            ts,
            price,
            category,
        }
    }
    #[test]
    fn layout_and_every_overflow_roundtrip() {
        for origin in [0_i64, 1_800_909_000_000, i64::MAX - 100, i64::MIN + 100] {
            let mut part = Part::with_capacity(0);
            for id in [0, 55, 1 << 63, u64::MAX] {
                for ingest in [
                    origin,
                    origin.wrapping_add(-1),
                    origin.wrapping_add(i64::from(i32::MAX)),
                    origin.wrapping_add(i64::from(i32::MIN)),
                    origin.wrapping_add(i64::from(i32::MAX) + 1),
                    origin.wrapping_add(i64::from(i32::MIN) - 1),
                    i64::MAX,
                    i64::MIN,
                ] {
                    for ts in [
                        origin,
                        origin.wrapping_add(1),
                        origin.wrapping_add(i64::from(INVALID) - 1),
                        origin.wrapping_add(i64::from(INVALID)),
                        origin.wrapping_sub(1),
                        BAD,
                        i64::MAX,
                    ] {
                        for (price, category) in [(0, 0), (1, 0), (0, 1), (u32::MAX, u32::MAX)] {
                            let e = event(u64::MAX - id, ts, price, category);
                            part.push(id, ingest, e, origin);
                            let row = part.rows.last().unwrap();
                            let got = part.event(row, origin);
                            assert_eq!(
                                (
                                    row.id,
                                    part.ingest(row, origin),
                                    got.user,
                                    got.ts,
                                    got.price,
                                    got.category
                                ),
                                (id, ingest, e.user, e.ts, e.price, e.category)
                            );
                        }
                    }
                }
            }
        }
    }
    #[test]
    fn exact_winners_across_fragments_and_representations() {
        let origin = 1_800_000_000_000i64;
        let mut parts: Vec<_> = (0..13).map(|_| Part::with_capacity(0)).collect();
        for i in 0..10000u64 {
            let mixed = crate::input::hash(i);
            let id = crate::input::hash(mixed % 1701);
            let ingest = origin.wrapping_add(i as i64);
            let ts = match mixed % 7 {
                0 => BAD,
                1 => i64::MAX,
                _ => origin + (mixed % 10000) as i64,
            };
            let price = if mixed % 5 == 0 { 20 } else { 0 };
            parts[mixed as usize % 13].push(id, ingest, event(id, ts, price, price), origin);
        }
        let full: Vec<_> = parts.iter().map(|p| p.expand(origin)).collect();
        let mut expected = vec![];
        let mut got = vec![];
        crate::singletons::select(&full, |e| {
            expected.push((e.user, e.ts, e.price, e.category))
        });
        select(&parts, origin, |e| {
            got.push((e.user, e.ts, e.price, e.category))
        });
        expected.sort_unstable();
        got.sort_unstable();
        assert_eq!(got, expected);
    }
}
