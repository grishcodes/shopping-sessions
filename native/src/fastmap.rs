//! Lean open-addressing map for `u64` keys on the per-event hot paths
//! (canonical-ID lookup, per-shard person grouping, ambiguous dedup IDs).
//!
//! One multiply to hash (Fibonacci hashing, high bits), linear probing,
//! power-of-two capacity at <= 50% load, and key `0` as the empty-slot
//! sentinel. A real key `0` is stored out of band, so the map is exact for
//! every `u64` key. Hash quality only affects probe lengths, never results.

const EMPTY: u64 = 0;
const MUL: u64 = 0x9E37_79B9_7F4A_7C15;

pub struct U64Map<V: Copy> {
    keys: Vec<u64>,
    vals: Vec<V>,
    shift: u32,
    mask: usize,
    len: usize,
    zero: Option<V>,
    filler: V,
}

impl<V: Copy> U64Map<V> {
    /// `filler` initialises unused value slots; it is never returned.
    pub fn with_capacity(n: usize, filler: V) -> Self {
        let slots = (n.max(4) * 2).next_power_of_two();
        Self {
            keys: vec![EMPTY; slots],
            vals: vec![filler; slots],
            shift: 64 - slots.trailing_zeros(),
            mask: slots - 1,
            len: 0,
            zero: None,
            filler,
        }
    }

    #[inline(always)]
    fn slot(&self, key: u64) -> usize {
        (key.wrapping_mul(MUL) >> self.shift) as usize
    }

    #[inline(always)]
    pub fn get(&self, key: u64) -> Option<V> {
        if key == EMPTY {
            return self.zero;
        }
        let mut i = self.slot(key);
        loop {
            // SAFETY: i is always masked to the table size.
            let k = unsafe { *self.keys.get_unchecked(i) };
            if k == key {
                return Some(unsafe { *self.vals.get_unchecked(i) });
            }
            if k == EMPTY {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Insert or overwrite.
    pub fn insert(&mut self, key: u64, value: V) {
        let _ = self.entry(key, || value);
        *self.get_mut(key).unwrap() = value;
    }

    fn get_mut(&mut self, key: u64) -> Option<&mut V> {
        if key == EMPTY {
            return self.zero.as_mut();
        }
        let mut i = self.slot(key);
        loop {
            let k = self.keys[i];
            if k == key {
                return Some(&mut self.vals[i]);
            }
            if k == EMPTY {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// The value for `key`, inserting `make()` first if absent. Returns the
    /// value and whether it was inserted by this call.
    #[inline(always)]
    pub fn entry(&mut self, key: u64, make: impl FnOnce() -> V) -> (V, bool) {
        if key == EMPTY {
            return match self.zero {
                Some(v) => (v, false),
                None => {
                    let v = make();
                    self.zero = Some(v);
                    (v, true)
                }
            };
        }
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut i = self.slot(key);
        loop {
            // SAFETY: i is always masked to the table size.
            let k = unsafe { *self.keys.get_unchecked(i) };
            if k == key {
                return (unsafe { *self.vals.get_unchecked(i) }, false);
            }
            if k == EMPTY {
                let v = make();
                unsafe {
                    *self.keys.get_unchecked_mut(i) = key;
                    *self.vals.get_unchecked_mut(i) = v;
                }
                self.len += 1;
                return (v, true);
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Overwrite the value of a key that is already present.
    #[inline(always)]
    pub fn set(&mut self, key: u64, value: V) {
        if let Some(v) = self.get_mut(key) {
            *v = value;
        }
    }

    #[cold]
    fn grow(&mut self) {
        let mut bigger = Self::with_capacity(self.keys.len(), self.filler);
        bigger.zero = self.zero;
        for (i, &k) in self.keys.iter().enumerate() {
            if k != EMPTY {
                let mut j = bigger.slot(k);
                while bigger.keys[j] != EMPTY {
                    j = (j + 1) & bigger.mask;
                }
                bigger.keys[j] = k;
                bigger.vals[j] = self.vals[i];
                bigger.len += 1;
            }
        }
        *self = bigger;
    }

    pub fn len(&self) -> usize {
        self.len + usize::from(self.zero.is_some())
    }

    /// All (key, value) pairs, in unspecified order.
    pub fn iter(&self) -> impl Iterator<Item = (u64, V)> + '_ {
        self.zero.map(|v| (EMPTY, v)).into_iter().chain(
            self.keys
                .iter()
                .zip(&self.vals)
                .filter(|(k, _)| **k != EMPTY)
                .map(|(k, v)| (*k, *v)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn agrees_with_std_hashmap_including_zero_and_growth() {
        let mut m = U64Map::with_capacity(1, 0u32);
        let mut r = HashMap::new();
        let mut x = 0x1234_5678_9abc_def0u64;
        for i in 0..200_000u32 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            // Small key space forces repeats; include 0 and u64::MAX.
            let key = match i % 97 {
                0 => 0,
                1 => u64::MAX,
                _ => x % 50_000,
            };
            let (v, fresh) = m.entry(key, || i);
            let expected = *r.entry(key).or_insert(i);
            assert_eq!(v, expected);
            assert_eq!(fresh, expected == i);
            if i % 5 == 0 {
                m.set(key, i + 1);
                r.insert(key, i + 1);
            }
        }
        assert_eq!(m.len(), r.len());
        for (k, v) in &r {
            assert_eq!(m.get(*k), Some(*v));
        }
        assert_eq!(m.get(123_456_789), None);
        let mut seen: Vec<_> = m.iter().collect();
        seen.sort();
        let mut want: Vec<_> = r.into_iter().collect();
        want.sort();
        assert_eq!(seen, want);
    }
}
