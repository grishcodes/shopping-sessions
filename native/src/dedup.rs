use super::{CopyOfEvent, input::hash, tables::Event};

#[derive(Clone, Copy, Default)]
struct Index {
    tag: u32,
    row: u32,
}

// The caller's shard is chosen using low hash bits. Use the independent upper
// bits here, and always verify the exact interned/packed ID before discarding.
#[inline]
fn tag(copy: &CopyOfEvent) -> u32 {
    (hash(copy.id) >> 32) as u32
}

#[inline]
fn earlier(copies: &[CopyOfEvent], a: u32, b: u32, discard: &mut [bool]) -> u32 {
    let (winner, loser) = if copies[a as usize].ingest <= copies[b as usize].ingest {
        (a, b)
    } else {
        (b, a)
    };
    discard[loser as usize] = true;
    winner
}

fn compact_hash(copies: &[CopyOfEvent], discard: &mut [bool]) {
    let slots = (copies.len() + copies.len().div_ceil(2)).next_power_of_two();
    let mut table = vec![
        Index {
            tag: 0,
            row: u32::MAX
        };
        slots
    ];
    for (row, copy) in copies.iter().enumerate() {
        let tag = tag(copy);
        let mut slot = tag as usize & (slots - 1);
        loop {
            let entry = &mut table[slot];
            if entry.row == u32::MAX {
                *entry = Index {
                    tag,
                    row: row as u32,
                };
                break;
            }
            if entry.tag == tag && copies[entry.row as usize].id == copy.id {
                entry.row = earlier(copies, entry.row, row as u32, discard);
                break;
            }
            slot = (slot + 1) & (slots - 1);
        }
    }
}

fn radix(copies: &[CopyOfEvent], discard: &mut [bool]) {
    // Sort 8-byte indices by a 24-bit fingerprint. For a 20M dataset and 128
    // shards this gives fewer than one accidental collision per hundred rows.
    let mut histograms = [[0usize; 256]; 3];
    let mut source = Vec::with_capacity(copies.len());
    for (row, copy) in copies.iter().enumerate() {
        let tag = tag(copy) >> 8;
        for (pass, counts) in histograms.iter_mut().enumerate() {
            counts[((tag >> (pass * 8)) & 255) as usize] += 1;
        }
        source.push(Index {
            tag,
            row: row as u32,
        });
    }
    let mut target = vec![Index::default(); copies.len()];
    for (pass, counts) in histograms.iter_mut().enumerate() {
        let mut offset = 0;
        for count in counts.iter_mut() {
            let n = *count;
            *count = offset;
            offset += n;
        }
        for entry in source.iter().copied() {
            let slot = &mut counts[((entry.tag >> (pass * 8)) & 255) as usize];
            target[*slot] = entry;
            *slot += 1;
        }
        std::mem::swap(&mut source, &mut target);
    }
    for group in source.chunk_by_mut(|a, b| a.tag == b.tag) {
        match group {
            [_] => {}
            [a, b] => {
                if copies[a.row as usize].id == copies[b.row as usize].id {
                    earlier(copies, a.row, b.row, discard);
                }
            }
            multiple => {
                multiple.sort_unstable_by_key(|entry| copies[entry.row as usize].id);
                for same in
                    multiple.chunk_by(|a, b| copies[a.row as usize].id == copies[b.row as usize].id)
                {
                    let mut winner = same[0].row;
                    for entry in &same[1..] {
                        winner = earlier(copies, winner, entry.row, discard);
                    }
                }
            }
        }
    }
}

/// Emit earliest ingests, including invalid winners. Emission preserves input
/// order for locality when the caller canonicalizes and repartitions events.
pub(super) fn select(copies: &[CopyOfEvent], mode: &str, mut emit: impl FnMut(Event)) {
    assert!(
        copies.len() <= u32::MAX as usize,
        "shard exceeds 32-bit row index"
    );
    let mut discard = vec![false; copies.len()];
    match mode {
        "radix" => radix(copies, &mut discard),
        "compact-hash" => compact_hash(copies, &mut discard),
        _ => panic!("unknown compact deduplication mode {mode}"),
    }
    for (copy, discarded) in copies.iter().zip(discard) {
        if !discarded {
            emit(copy.event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn row(id: u64, ingest: i64, ts: i64) -> CopyOfEvent {
        CopyOfEvent {
            id,
            ingest,
            event: Event {
                user: id,
                ts,
                price: 0,
                category: 0,
            },
        }
    }

    fn compare(copies: &[CopyOfEvent]) {
        let mut baseline = BTreeMap::new();
        for copy in copies {
            let best = baseline.entry(copy.id).or_insert(copy);
            if copy.ingest < best.ingest {
                *best = copy;
            }
        }
        let expected: Vec<_> = baseline
            .into_iter()
            .map(|(id, copy)| (id, copy.event.ts))
            .collect();
        for mode in ["radix", "compact-hash"] {
            let mut got = Vec::new();
            select(copies, mode, |e| got.push((e.user, e.ts)));
            got.sort_unstable();
            assert_eq!(got, expected, "{mode}");
        }
    }

    #[test]
    fn fingerprint_collisions_and_invalid_earliest() {
        let (a, b) = (23901, 52826);
        assert_eq!(hash(a) >> 32, hash(b) >> 32);
        compare(&[]);
        compare(&[row(a, 1, 1)]);
        compare(&[row(a, 1, 1), row(b, 2, 2)]);
        compare(&[
            row(a, 3, 3),
            row(b, 2, 2),
            row(a, 2, 2),
            row(b, 1, 1),
            row(a, -1, i64::MIN),
            row(b, 1, 1),
            row(u64::MAX, 8, 8),
        ]);
    }

    #[test]
    fn matches_ordered_map_on_scrambled_inputs() {
        let mut rows: Vec<_> = (1..16_384u64)
            .map(|i| {
                let mixed = hash(i);
                let id = hash(mixed % 4301);
                let ingest = (mixed % 301) as i64;
                row(id, ingest, if ingest % 11 == 0 { i64::MIN } else { ingest })
            })
            .collect();
        compare(&rows);
        rows.reverse();
        compare(&rows);
    }
}
