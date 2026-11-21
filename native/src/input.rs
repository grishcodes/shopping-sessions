pub use crate::scan::rows;
use ahash::AHashMap;
use memchr::memchr;
use memmap2::Mmap;
use std::{
    fs::{self, File},
    io,
    path::Path,
    sync::Mutex,
};

pub const BAD: i64 = i64::MIN;
pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub type Key = u64;
const LONG: Key = 1 << 63;

#[derive(Default)]
struct Dictionary {
    ids: AHashMap<Vec<u8>, Key>,
    names: Vec<Vec<u8>>,
}
#[derive(Default)]
pub struct Keys(Mutex<Dictionary>);
pub struct Names(Vec<Vec<u8>>);
impl Keys {
    pub fn id(&self, s: &[u8]) -> Key {
        if s.is_empty() {
            return 0;
        }
        if s.len() == 9 && s[0] > 0 && s[0] < 128 {
            let bytes = u64::from_be_bytes(s[1..].try_into().unwrap());
            let high = 0x8080_8080_8080_8080;
            let zeros = bytes.wrapping_sub(0x0101_0101_0101_0101) & !bytes & high;
            if bytes & high == 0 && zeros == 0 {
                #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
                {
                    // PEXT retains the seven payload bits in each byte in order.
                    let payload = unsafe { std::arch::x86_64::_pext_u64(bytes, !high) };
                    return (u64::from(s[0]) << 56) | payload;
                }
                #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
                {
                    // Remove each byte's zero high bit, then combine pairs, quads and octets.
                    let pairs =
                        (bytes & 0x007f_007f_007f_007f) | ((bytes & 0x7f00_7f00_7f00_7f00) >> 1);
                    let quads =
                        (pairs & 0x0000_3fff_0000_3fff) | ((pairs & 0x3fff_0000_3fff_0000) >> 2);
                    return (u64::from(s[0]) << 56) | (quads & 0x0fff_ffff) | ((quads >> 32) << 28);
                }
            }
        }
        if s.len() <= 9 {
            let mut value = 0u64;
            let mut fits = true;
            for &byte in s {
                fits &= byte != 0 && byte < 128;
                value = (value << 7) | u64::from(byte);
            }
            if fits {
                return value << (7 * (9 - s.len()));
            }
        }
        let mut dict = self.0.lock().unwrap();
        if let Some(&k) = dict.ids.get(s) {
            return k;
        }
        let k = LONG | dict.names.len() as u64;
        dict.ids.insert(s.to_vec(), k);
        dict.names.push(s.to_vec());
        k
    }
    pub fn names(&self) -> Names {
        Names(self.0.lock().unwrap().names.clone())
    }
}
impl Names {
    pub fn bytes<'a>(&'a self, key: Key, buf: &'a mut [u8; 9]) -> &'a [u8] {
        if key & LONG != 0 {
            return &self.0[(key ^ LONG) as usize];
        }
        let mut k = key;
        let mut len = 0;
        while k != 0 {
            buf[len] = (k >> 56) as u8;
            k = (k << 7) & !LONG;
            len += 1;
        }
        &buf[..len]
    }
    pub fn less(&self, a: Key, b: Key) -> bool {
        if (a | b) & LONG == 0 {
            return a < b;
        }
        self.bytes(a, &mut [0; 9]) < self.bytes(b, &mut [0; 9])
    }
}
pub struct Files(Vec<Mmap>);
impl Files {
    pub fn open(dir: &Path) -> io::Result<Self> {
        let mut maps = Vec::new();
        for entry in fs::read_dir(dir)? {
            let path = entry?.path();
            if path.extension().and_then(|s| s.to_str()) != Some("csv") {
                continue;
            }
            let f = File::open(path)?;
            if f.metadata()?.len() != 0 {
                // Scorer inputs stay read-only for the lifetime of the map.
                let map = unsafe { Mmap::map(&f)? };
                // Best-effort: start async readahead of the whole file now
                // (later files get read while earlier ones parse) and hint
                // for huge pages to cut TLB misses on first touch. The
                // kernel may ignore either; errors are not fatal.
                #[cfg(unix)]
                unsafe {
                    libc::madvise(
                        map.as_ptr() as *mut libc::c_void,
                        map.len(),
                        libc::MADV_WILLNEED,
                    );
                    libc::madvise(
                        map.as_ptr() as *mut libc::c_void,
                        map.len(),
                        libc::MADV_HUGEPAGE,
                    );
                }
                maps.push(map);
            }
        }
        Ok(Self(maps))
    }
    pub fn chunks(&self) -> Vec<&[u8]> {
        let mut chunks = Vec::new();
        for map in &self.0 {
            if let Some(lf) = memchr(b'\n', map) {
                let mut s = &map[lf + 1..];
                while !s.is_empty() {
                    let cut = 1 << 21;
                    let end = s
                        .get(cut..)
                        .and_then(|tail| memchr(b'\n', tail))
                        .map_or(s.len(), |pos| cut + pos + 1);
                    chunks.push(&s[..end]);
                    s = &s[end..];
                }
            }
        }
        chunks
    }
}
pub fn dimension(path: &Path, mut visit: impl FnMut(&[u8], &[u8])) -> io::Result<()> {
    let bytes = fs::read(path)?;
    if let Some(header) = memchr(b'\n', &bytes) {
        rows(&bytes[header + 1..], |[a, b]| visit(a, b));
    }
    Ok(())
}
pub fn digits(s: &[u8], limit: i64) -> i64 {
    if s.is_empty() {
        return BAD;
    }
    if s.len() <= 18 {
        let mut value = 0i64;
        let mut valid = true;
        for &b in s {
            let digit = b.wrapping_sub(b'0');
            valid &= digit < 10;
            value = value.wrapping_mul(10).wrapping_add(i64::from(digit));
        }
        return if valid && value <= limit { value } else { BAD };
    }
    let mut value = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return BAD;
        }
        let d = i64::from(b - b'0');
        if value > (limit - d) / 10 {
            return BAD;
        }
        value = value * 10 + d;
    }
    value
}
#[inline(always)]
fn pair(s: &[u8], i: usize) -> i64 {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        100
    } else {
        i64::from(a * 10 + b)
    }
}
pub fn stamp(s: &[u8]) -> i64 {
    if !matches!(s.len(), 20 | 25) || s[4] != b'-' {
        return digits(s, i64::MAX);
    }
    if s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return BAD;
    }
    let (century, yy, m, d) = (pair(s, 0), pair(s, 2), pair(s, 5), pair(s, 8));
    let (h, min, sec) = (pair(s, 11), pair(s, 14), pair(s, 17));
    if century > 99 || yy > 99 || !(1..=12).contains(&m) || h >= 24 || min >= 60 || sec >= 60 {
        return BAD;
    }
    let y = century * 100 + yy;
    let lengths = [
        31,
        28 + i64::from(y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)),
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if d < 1 || d > lengths[(m - 1) as usize] {
        return BAD;
    }
    let offset = if s.len() == 20 {
        if s[19] != b'Z' {
            return BAD;
        }
        0
    } else {
        let (oh, om) = (pair(s, 20), pair(s, 23));
        if oh >= 24 || om >= 60 || s[22] != b':' {
            return BAD;
        }
        match s[19] {
            b'+' => oh * 60 + om,
            b'-' => -oh * 60 - om,
            _ => return BAD,
        }
    };
    let year = y - i64::from(m <= 2);
    let era = year.div_euclid(400);
    let yo = year - 400 * era;
    let days = era * 146097 + yo * 365 + yo / 4 - yo / 100
        + (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5
        + d
        - 1
        - 719468;
    ((days * 1440 + h * 60 + min - offset) * 60 + sec) * 1000
}
pub fn hash(mut x: Key) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

// Ingest timestamps are guaranteed to be valid UTC whole seconds. Cache the
// window's starting month while retaining the general parser for other months.
pub struct Month {
    pub(crate) prefix: [u8; 8],
    pub(crate) base: i64,
    pub(crate) length: i64,
    // Second month: the window is 14 days, so it can straddle a month
    // boundary and ~half the rows then fall in the next month. Cache it
    // too, so those rows keep the base+offset path instead of stamp().
    pub(crate) prefix2: [u8; 8],
    pub(crate) base2: i64,
    pub(crate) length2: i64,
}
impl Month {
    pub fn from_start(s: &[u8]) -> Self {
        let mut first = *b"2000-01-01T00:00:00Z";
        first[..8].copy_from_slice(&s[..8]);
        let base = stamp(&first);
        first[8..10].copy_from_slice(b"31");
        let length = if stamp(&first) != BAD {
            31
        } else {
            first[8..10].copy_from_slice(b"30");
            if stamp(&first) != BAD {
                30
            } else {
                first[8..10].copy_from_slice(b"29");
                if stamp(&first) != BAD { 29 } else { 28 }
            }
        };
        // Next month: same stamp-trial for its length, base follows directly.
        let y = i64::from(s[0] - b'0') * 1000
            + i64::from(s[1] - b'0') * 100
            + i64::from(s[2] - b'0') * 10
            + i64::from(s[3] - b'0');
        let m = i64::from(s[5] - b'0') * 10 + i64::from(s[6] - b'0');
        let (y2, m2) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
        let mut prefix2 = *b"2000-01-";
        prefix2[0] = b'0' + (y2 / 1000) as u8;
        prefix2[1] = b'0' + ((y2 / 100) % 10) as u8;
        prefix2[2] = b'0' + ((y2 / 10) % 10) as u8;
        prefix2[3] = b'0' + (y2 % 10) as u8;
        prefix2[5] = b'0' + (m2 / 10) as u8;
        prefix2[6] = b'0' + (m2 % 10) as u8;
        let base2 = base + length * DAY;
        let mut second = *b"2000-01-01T00:00:00Z";
        second[..8].copy_from_slice(&prefix2);
        second[8..10].copy_from_slice(b"31");
        let length2 = if stamp(&second) != BAD {
            31
        } else {
            second[8..10].copy_from_slice(b"30");
            if stamp(&second) != BAD {
                30
            } else {
                second[8..10].copy_from_slice(b"29");
                if stamp(&second) != BAD { 29 } else { 28 }
            }
        };
        Self {
            prefix: s[..8].try_into().unwrap(),
            base,
            length,
            prefix2,
            base2,
            length2,
        }
    }
    /// (base, length) for the month whose `YYYY-MM-` prefix matches, if either
    /// cached month does.
    pub(crate) fn base_len(&self, prefix: &[u8]) -> Option<(i64, i64)> {
        if prefix == self.prefix {
            Some((self.base, self.length))
        } else if prefix == self.prefix2 {
            Some((self.base2, self.length2))
        } else {
            None
        }
    }
    pub fn ingest(&self, s: &[u8]) -> i64 {
        if s.len() != 20 {
            return stamp(s);
        }
        let base = match self.base_len(&s[..8]) {
            Some((base, _)) => base,
            None => return stamp(s),
        };
        let two = |at: usize| i64::from(s[at] - b'0') * 10 + i64::from(s[at + 1] - b'0');
        base + (two(8) - 1) * DAY + ((two(11) * 60 + two(14)) * 60 + two(17)) * 1000
    }
    pub fn event(&self, s: &[u8]) -> i64 {
        if s.len() != 20 {
            return stamp(s);
        }
        let (base, length) = match self.base_len(&s[..8]) {
            Some(v) => v,
            None => return stamp(s),
        };
        if s[10] != b'T' || s[13] != b':' || s[16] != b':' || s[19] != b'Z' {
            return BAD;
        }
        let (d, h, m, sec) = (pair(s, 8), pair(s, 11), pair(s, 14), pair(s, 17));
        if d == 0 || d > length || h >= 24 || m >= 60 || sec >= 60 {
            return BAD;
        }
        base + (d - 1) * DAY + ((h * 60 + m) * 60 + sec) * 1000
    }
}
