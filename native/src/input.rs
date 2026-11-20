use ahash::AHashMap;
use memchr::{memchr, memchr_iter};
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
                maps.push(unsafe { Mmap::map(&f)? });
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
#[inline]
pub fn rows<const N: usize>(chunk: &[u8], mut visit: impl FnMut([&[u8]; N])) {
    let mut start = 0;
    for end in memchr_iter(b'\n', chunk).chain(std::iter::once(chunk.len())) {
        if end > start {
            let line = &chunk[start..end];
            let mut fields = [b"".as_slice(); N];
            let mut it = line.split(|&c| c == b',');
            let mut valid = true;
            for field in &mut fields {
                let Some(mut value) = it.next() else {
                    valid = false;
                    break;
                };
                if value.len() >= 2 && value[0] == b'"' && value.last() == Some(&b'"') {
                    value = &value[1..value.len() - 1];
                }
                if value.contains(&b'"') {
                    valid = false;
                    break;
                }
                *field = value;
            }
            if valid && it.next().is_none() {
                visit(fields);
            }
        }
        start = end + 1;
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
