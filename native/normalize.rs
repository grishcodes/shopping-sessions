//! Validate the contest's LF-delimited log files before a standard CSV scan.
use std::env;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

fn body(field: &[u8]) -> Option<&[u8]> {
    let unquoted = match field {
        [b'"', middle @ .., b'"'] => middle,
        _ => field,
    };
    (!unquoted.contains(&b'"')).then_some(unquoted)
}

fn normalize(source: &Path, target: &Path, width: usize) -> io::Result<()> {
    let mut reader = BufReader::with_capacity(128 * 1024, File::open(source)?);
    let mut writer = BufWriter::with_capacity(128 * 1024, File::create(target)?);
    let mut line = Vec::with_capacity(256);
    reader.read_until(b'\n', &mut line)?; // Skip the header, even in empty parts.
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        let row = line.strip_suffix(b"\n").unwrap_or(&line);
        if row.iter().filter(|&&byte| byte == b',').count() != width - 1 {
            continue;
        }
        if row.contains(&b'"') && !row.split(|&byte| byte == b',').all(|field| body(field).is_some()) {
            continue;
        }
        if row.contains(&b'\r') {
            for (index, field) in row.split(|&byte| byte == b',').enumerate() {
                writer.write_all(if index == 0 { b"\"" } else { b",\"" })?;
                writer.write_all(body(field).expect("validated quoting"))?;
                writer.write_all(b"\"")?;
            }
        } else {
            writer.write_all(row)?;
        }
        writer.write_all(b"\n")?;
    }
    writer.flush()
}

fn main() -> io::Result<()> {
    let arguments: Vec<_> = env::args().collect();
    if arguments.len() != 5 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: normalize INPUT_DIRECTORY OUTPUT_DIRECTORY FIELD_COUNT THREADS",
        ));
    }
    let source = Path::new(&arguments[1]);
    let target = Path::new(&arguments[2]);
    let width: usize = arguments[3].parse().expect("numeric field count");
    let workers: usize = arguments[4].parse().expect("numeric thread count");
    assert!(width >= 2 && workers >= 1);
    let mut files = Vec::new();
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "csv") {
            files.push((entry.metadata()?.len(), path));
        }
    }
    // Start the largest shards first to reduce idle time near the end.
    files.sort_unstable_by(|left, right| right.0.cmp(&left.0));
    let next = AtomicUsize::new(0);
    thread::scope(|scope| {
        let handles: Vec<_> = (0..workers.min(files.len()))
            .map(|_| {
                scope.spawn(|| {
                    while let Some((_, path)) = files.get(next.fetch_add(1, Ordering::Relaxed)) {
                        normalize(path, &target.join(path.file_name().unwrap()), width)?;
                    }
                    Ok::<_, io::Error>(())
                })
            })
            .collect();
        for handle in handles {
            handle.join().expect("normalization worker panicked")?;
        }
        Ok(())
    })
}
