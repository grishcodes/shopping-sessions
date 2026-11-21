//! Stable LSD radix sort over 16-bit digits (counting sort per pass).
//! Used to order a shard's events and clicks by (user, ts) without a
//! hash map and without per-person comparison sorts. Each pass is a
//! sequential read plus a bucketed scatter, so the whole sort is O(n)
//! with small constants; the whale person that dominates one shard costs
//! no more than any other row.

/// Stable LSD radix sort of `items` by the `digits` 16-bit digits returned
/// by `digit` (least significant first). `aux` is scratch of equal length.
/// After the call the sorted data is in `items`. Callers should prefer an
/// even digit count so no copy-back is needed (an odd count copies back).
pub fn lsd_sort<T: Copy>(
    items: &mut [T],
    aux: &mut [T],
    digits: usize,
    digit: impl Fn(&T, usize) -> u16,
) {
    assert_eq!(items.len(), aux.len());
    if items.len() < 2 || digits == 0 {
        return;
    }
    let mut hist = vec![0u32; 65536];
    let mut off = vec![0u32; 65536];
    let mut src_is_items = true;
    for pass in 0..digits {
        hist.fill(0);
        if src_is_items {
            for item in items.iter() {
                hist[digit(item, pass) as usize] += 1;
            }
        } else {
            for item in aux.iter() {
                hist[digit(item, pass) as usize] += 1;
            }
        }
        let mut sum = 0u32;
        for (h, o) in hist.iter().zip(off.iter_mut()) {
            *o = sum;
            sum += *h;
        }
        if src_is_items {
            for item in items.iter() {
                let d = digit(item, pass) as usize;
                let pos = off[d] as usize;
                off[d] += 1;
                aux[pos] = *item;
            }
        } else {
            for item in aux.iter() {
                let d = digit(item, pass) as usize;
                let pos = off[d] as usize;
                off[d] += 1;
                items[pos] = *item;
            }
        }
        src_is_items = !src_is_items;
    }
    if !src_is_items {
        items.copy_from_slice(aux);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorts_by_two_key_digits() {
        let mut data: Vec<(u64, u64)> = (0..100_000)
            .map(|i| {
                let x = (i * 2654435761) % 100_003;
                (x % 997, x % 89)
            })
            .collect();
        let mut aux = vec![(0, 0); data.len()];
        // Sort by (first, second): least significant digit is `second`.
        lsd_sort(&mut data, &mut aux, 2, |v, p| {
            if p == 0 {
                (v.1 & 0xFFFF) as u16
            } else {
                (v.0 & 0xFFFF) as u16
            }
        });
        let mut expected: Vec<(u64, u64)> = (0..100_000)
            .map(|i| {
                let x = (i * 2654435761) % 100_003;
                (x % 997, x % 89)
            })
            .collect();
        expected.sort_unstable();
        assert_eq!(data, expected);
    }
}
