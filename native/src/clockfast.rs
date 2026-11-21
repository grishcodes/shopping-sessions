//! Specialized timestamp paths for common event and click representations.
use crate::input::{BAD, DAY, Month};

/// Return the value of eight decimal bytes in little-endian order.
/// Each reduction combines adjacent decimal groups without carries between lanes.
#[inline(always)]
fn eight(digits: u64) -> u32 {
    const EVEN: u64 = 0x000f_000f_000f_000f;
    let pairs = (digits & EVEN) * 10 + ((digits >> 8) & EVEN);
    const PAIRS: u64 = 0x0000_00ff_0000_00ff;
    let fours = (pairs & PAIRS) * 100 + ((pairs >> 16) & PAIRS);
    (fours as u32) * 10_000 + (fours >> 32) as u32
}

#[inline(always)]
fn epoch_13(s: &[u8; 13]) -> i64 {
    // Read exactly thirteen bytes. The first four and last eight form separate
    // little-endian words, with one decimal digit between them.
    let head = u32::from_le_bytes(s[..4].try_into().unwrap());
    let mid = s[4].wrapping_sub(b'0');
    let tail = u64::from_le_bytes(s[5..].try_into().unwrap());
    let head_hi = head ^ 0x3030_3030;
    let tail_hi = tail ^ 0x3030_3030_3030_3030;
    let wrong_zone = (u64::from(head_hi) | tail_hi) & 0xf0f0_f0f0_f0f0_f0f0;
    // Low nibbles 10..15 overflow into bit 4 when incremented by six.
    let head_check = (head & 0x0f0f_0f0f) + 0x0606_0606;
    let tail_check = (tail & 0x0f0f_0f0f_0f0f_0f0f) + 0x0606_0606_0606_0606;
    if wrong_zone != 0
        || (u64::from(head_check) | tail_check) & 0x1010_1010_1010_1010 != 0
        || mid >= 10
    {
        return BAD;
    }
    let head_pairs = (head & 0x000f_000f) * 10 + ((head >> 8) & 0x000f_000f);
    let first_four = (head_pairs & 0xff) * 100 + (head_pairs >> 16);
    i64::from(first_four * 10 + u32::from(mid)) * 100_000_000 + i64::from(eight(tail))
}

#[inline(always)]
fn pair(s: &[u8], i: usize) -> u32 {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        100
    } else {
        u32::from(a * 10 + b)
    }
}

#[inline]
pub fn event(month: &Month, s: &[u8]) -> i64 {
    if let Ok(digits) = <&[u8; 13]>::try_from(s) {
        return epoch_13(digits);
    }
    // F3 with leading zeros: strip them; exactly 13 digits left routes to
    // the SWAR path. Anything non-digit still yields BAD from epoch_13,
    // matching the general digits() path.
    if s.len() > 13 {
        let mut i = 0;
        while i < s.len() && s[i] == b'0' {
            i += 1;
        }
        if s.len() - i == 13 {
            if let Ok(digits) = <&[u8; 13]>::try_from(&s[i..]) {
                return epoch_13(digits);
            }
        }
    }
    let Some((base, length)) = (if s.len() == 25 {
        month.base_len(&s[..8])
    } else {
        None
    }) else {
        return month.event(s);
    };
    if s[10] != b'T' || s[13] != b':' || s[16] != b':' || s[22] != b':' {
        return BAD;
    }
    let day = pair(s, 8);
    let hour = pair(s, 11);
    let minute = pair(s, 14);
    let second = pair(s, 17);
    let offset_hour = pair(s, 20);
    let offset_minute = pair(s, 23);
    if day == 0
        || i64::from(day) > length
        || hour >= 24
        || minute >= 60
        || second >= 60
        || offset_hour >= 24
        || offset_minute >= 60
    {
        return BAD;
    }
    let offset = i64::from(offset_hour * 60 + offset_minute);
    let offset = match s[19] {
        b'+' => offset,
        b'-' => -offset,
        _ => return BAD,
    };
    let minutes = i64::from(hour * 60 + minute) - offset;
    base + (i64::from(day) - 1) * DAY + (minutes * 60 + i64::from(second)) * 1000
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::stamp;

    fn check(month: &Month, raw: &[u8]) {
        assert_eq!(event(month, raw), stamp(raw), "{raw:?}");
    }

    #[test]
    fn fixed_width_decimal_and_civil_boundaries() {
        let leap = Month::from_start(b"2028-02-20T00:00:00Z");
        let regular = Month::from_start(b"2027-02-20T00:00:00Z");
        let century = Month::from_start(b"2100-02-20T00:00:00Z");
        for month in [&leap, &regular, &century] {
            for input in [
                b"0000000000000".as_slice(),
                b"9999999999999",
                b"0246813579864",
                b"0000000000000000000000001",
                b"18446744073709551616",
                b"-000000000001",
                b"2028-02-01T00:00:00+23:59",
                b"2028-02-29T23:59:59-23:59",
                b"2028-02-29T00:00:00-00:00",
                b"2028-02-30T00:00:00+00:00",
                b"2027-02-29T00:00:00+01:30",
                b"2100-02-29T00:00:00+01:30",
                b"2028-02-28T23:59:60-00:00",
                b"2028-02-28T24:00:00+00:00",
                b"2028-02-28T23:59:59+24:00",
                b"2028-02-28T23:59:59-23:60",
                b"2028-01-31T23:59:59+00:01",
                b"2028-03-01T00:00:00+01:00",
            ] {
                check(month, input);
            }
        }
    }

    #[test]
    fn second_month_and_leading_zero_f3_match_general_parser() {
        // Window starting Feb 20 straddles into March (second cached month).
        let month = Month::from_start(b"2028-02-20T00:00:00Z");
        for input in [
            b"2028-03-01T00:00:00Z".as_slice(),
            b"2028-03-31T23:59:59Z",
            b"2028-03-01T00:00:00+01:00",
            b"2028-03-31T23:59:59-23:59",
            b"2028-03-32T00:00:00Z",
            b"2028-02-29T23:59:59Z",
            b"2028-04-01T00:00:00Z",
            b"0001791071890000",
            b"00000000000001791071890000",
            b"0000000000000000000000000",
            b"0000000000000000000000001",
            b"0000000000000a",
        ] {
            check(&month, input);
        }
        // December -> January year rollover for the second month.
        let dec = Month::from_start(b"2028-12-20T00:00:00Z");
        for input in [
            b"2029-01-01T00:00:00Z".as_slice(),
            b"2029-01-31T23:59:59Z",
            b"2029-01-31T23:59:59+05:30",
            b"2029-02-01T00:00:00Z",
            b"2028-12-31T23:59:59Z",
        ] {
            check(&dec, input);
        }
    }

    /// Timing probe, not a correctness test: reads a one-ts-per-line corpus
    /// from $TS_CORPUS with window start $TS_START and prints ns/parse.
    /// Skipped unless TS_CORPUS is set. Run with --nocapture.
    #[test]
    fn bench_ts_corpus() {
        let Ok(path) = std::env::var("TS_CORPUS") else {
            return;
        };
        let start = std::env::var("TS_START").unwrap();
        let raw = std::fs::read(path).unwrap();
        let lines: Vec<&[u8]> = raw
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .collect();
        let month = Month::from_start(start.as_bytes());
        let mut sink: i64 = 0;
        for line in &lines {
            sink = sink.wrapping_add(event(&month, line));
        }
        let iters = 50;
        let t0 = std::time::Instant::now();
        for _ in 0..iters {
            for line in &lines {
                sink = sink.wrapping_add(event(&month, line));
            }
        }
        let el = t0.elapsed();
        let total = lines.len() as u128 * iters as u128;
        eprintln!(
            "TSBENCH parses={total} secs={:.3} ns_per_parse={:.2} sink={sink}",
            el.as_secs_f64(),
            el.as_nanos() as f64 / total as f64
        );
    }

    #[test]
    fn every_byte_mutation_matches_general_parser() {
        let month = Month::from_start(b"2028-02-20T00:00:00Z");
        for template in [b"2028-02-29T22:59:59-04:01".as_slice(), b"1290863574159"] {
            let mut bytes = template.to_vec();
            for index in 0..bytes.len() {
                for value in 0..=255 {
                    bytes[index] = value;
                    check(&month, &bytes);
                }
                bytes[index] = template[index];
            }
        }
    }

    #[test]
    fn generated_epoch_and_offset_strings_match_general_parser() {
        let month = Month::from_start(b"2028-02-20T00:00:00Z");
        let mut state = 71_u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..4000 {
            let epoch = format!("{:013}", next() % 10_000_000_000_000);
            check(&month, epoch.as_bytes());
            let civil = format!(
                "2028-02-{:02}T{:02}:{:02}:{:02}{}{:02}:{:02}",
                next() % 33,
                next() % 25,
                next() % 61,
                next() % 61,
                if next() % 2 == 0 { '+' } else { '-' },
                next() % 25,
                next() % 61
            );
            check(&month, civil.as_bytes());
        }
    }
}
