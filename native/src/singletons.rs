use super::{
    CopyOfEvent,
    input::{Key, hash},
    tables::Event,
};
use ahash::AHashMap;

#[derive(Clone, Copy, Default)]
struct Bits {
    seen: u64,
    repeated: u64,
}

#[inline]
fn location(id: Key, word_mask: usize) -> (usize, u64) {
    let h = hash(id);
    // Skip the low bits already used to partition the input. The two bits
    // live in separate halves of one word, so one cache lookup checks both.
    let word = ((h >> 8) as usize) & word_mask;
    let low = 1u64 << ((h >> 40) & 31);
    let high = (1u64 << 32) << ((h >> 45) & 31);
    (word, low | high)
}

pub(super) fn select(parts: &[Vec<CopyOfEvent>], mut emit: impl FnMut(Event)) {
    let count: usize = parts.iter().map(Vec::len).sum();
    if count == 0 {
        return;
    }
    // Each word handles up to eight rows on average, using 2–4 bytes per
    // input row for both bitmaps together. A collision can only send extra
    // rows to exact lookup, never cause unrelated rows to be discarded.
    let words = count.div_ceil(8).next_power_of_two();
    let word_mask = words - 1;
    let mut bits = vec![Bits::default(); words];
    for copy in parts.iter().flatten() {
        let (word, mask) = location(copy.id, word_mask);
        let b = &mut bits[word];
        b.repeated |= b.seen & mask;
        b.seen |= mask;
    }

    let mut ambiguous = AHashMap::with_capacity(count / 16);
    for copy in parts.iter().flatten() {
        let (word, mask) = location(copy.id, word_mask);
        if bits[word].repeated & mask != mask {
            emit(copy.event);
        } else {
            // All copies of an exact ID reach this branch together. Keep
            // invalid events eligible here; the caller validates the winner.
            let winner = ambiguous.entry(copy.id).or_insert(copy);
            if copy.ingest < winner.ingest {
                *winner = copy;
            }
        }
    }
    for winner in ambiguous.into_values() {
        emit(winner.event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: Key, ingest: i64, ts: i64) -> CopyOfEvent {
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

    fn compare(parts: &[Vec<CopyOfEvent>]) {
        // The original sorted picker serves as an oracle for both ordering
        // and the winning payload. Its input includes invalid event copies.
        let mut copies: Vec<_> = parts.iter().flatten().copied().collect();
        copies.sort_unstable_by_key(|copy| copy.id);
        let expected: Vec<_> = copies
            .chunk_by(|a, b| a.id == b.id)
            .map(|same| {
                let winner = same.iter().min_by_key(|copy| copy.ingest).unwrap();
                (winner.id, winner.event.ts)
            })
            .collect();
        let mut actual = Vec::new();
        select(parts, |evt| actual.push((evt.user, evt.ts)));
        actual.sort_unstable();
        assert_eq!(actual, expected);
    }

    #[test]
    fn handles_empty_parts_and_false_positives() {
        compare(&[]);
        compare(&[vec![], vec![]]);
        // Five distinct IDs with the exact same two bits in a one-word
        // bitmap. Every candidate goes through the exact lookup fallback.
        let target = location(1, 0);
        let ids: Vec<_> = (1..)
            .filter(|&id| location(id, 0) == target)
            .take(5)
            .collect();
        let mut parts = vec![ids.iter().map(|&id| row(id, 12, 12)).collect(), vec![]];
        parts[1].push(row(ids[0], 2, i64::MIN));
        parts[1].push(row(ids[0], 2, i64::MIN));
        compare(&parts);
    }

    #[test]
    fn agrees_with_sorted_picker_across_fragmented_shards() {
        for modulus in [41, 4093, 32771] {
            let mut parts: Vec<Vec<_>> = (0..37).map(|_| Vec::new()).collect();
            for i in 1..20_000u64 {
                let mixed = hash(i);
                let id = hash(mixed % modulus);
                let ingest = (mixed % 227) as i64;
                let ts = if ingest % 11 == 0 { i64::MIN } else { ingest };
                let bucket = ((mixed >> 20) % 37) as usize;
                parts[bucket].push(row(id, ingest, ts));
            }
            compare(&parts);
            parts.reverse();
            compare(&parts);
        }
    }
}
