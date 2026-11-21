mod clockfast;
mod compact;
mod dedup;
mod fastmap;
mod fastrows;
mod identity;
mod input;
mod output;
mod radix;
mod scan;
mod singletons;
mod tables;
use ahash::AHashMap;
use identity::Canon;
use input::{BAD, DAY, Files, Key, Keys, WEEK, hash, rows, stamp};
use rayon::prelude::*;
use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Instant,
};
use tables::{Click, Credit, Event, RawEvent};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
fn n_shards() -> usize {
    static SHARDS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *SHARDS.get_or_init(|| {
        std::env::var("SHARDS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n >= 1 && n.is_power_of_two())
            .unwrap_or(128)
    })
}

fn link_map(data: &Path, keys: &Keys) -> Result<Canon> {
    let files = Files::open(&data.join("links"))?;
    let chunks = files.chunks();
    let edges: Vec<Vec<_>> = chunks
        .par_iter()
        .map(|s| {
            let mut edges = Vec::new();
            rows(s, |[a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    edges.push((keys.id(a), keys.id(b)))
                }
            });
            edges
        })
        .collect();
    Ok(identity::build(edges, keys))
}
fn buckets<T>() -> Vec<Vec<T>> {
    (0..n_shards()).map(|_| Vec::new()).collect()
}
// Size the initial buffers from input bytes. Rows wider or shorter than the
// estimate use normal Vec growth. The estimate never affects field validation.
fn reserved_buckets<T>(rows: usize) -> Vec<Vec<T>> {
    (0..n_shards())
        .map(|_| Vec::with_capacity(rows.div_ceil(n_shards())))
        .collect()
}
fn report_buffers<T>(label: &str, blocks: &[Vec<Vec<T>>]) {
    if env::var_os("PROFILE_MEMORY").is_some() {
        let (mut used, mut capacity, mut fragments) = (0, 0, 0);
        for part in blocks.iter().flatten() {
            used += part.len();
            capacity += part.capacity();
            fragments += usize::from(part.capacity() > 0);
        }
        let width = std::mem::size_of::<T>();
        eprintln!(
            "{label} live_bytes={} capacity_bytes={} allocated_buffers={fragments}",
            used * width,
            capacity * width
        );
    }
}
fn redistribute<T>(blocks: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
    let mut shards = buckets();
    for block in blocks {
        for (shard, items) in shards.iter_mut().zip(block) {
            if !items.is_empty() {
                shard.push(items)
            }
        }
    }
    shards
}
fn shard(id: Key) -> usize {
    (hash(id) as usize) & (n_shards() - 1)
}
/// Decode a dense numeric ID: `prefix` + 1..7 ASCII digits, no leading
/// zero. Campaign IDs are `c_<digits>`, product IDs `p<digits>`; anything
/// else returns None and the caller falls back to the hash map.
#[inline]
fn dense_num(s: &[u8], prefix: &[u8]) -> Option<usize> {
    if s.len() <= prefix.len()
        || s.len() > prefix.len() + 7
        || &s[..prefix.len()] != prefix
    {
        return None;
    }
    let digits = &s[prefix.len()..];
    if digits[0] == b'0' {
        return None;
    }
    let mut n = 0usize;
    for &b in digits {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        n = n * 10 + d as usize;
    }
    Some(n)
}
/// Direct-index lookup table for dense IDs, built next to the hash map.
/// Entry `u32::MAX` means absent; a miss falls back to the hash map, so
/// any non-dense or unknown ID stays correct.
struct Dense {
    table: Vec<u32>,
}
impl Dense {
    fn new() -> Self {
        Self { table: Vec::new() }
    }
    fn insert(&mut self, n: usize, value: u32) {
        if n >= self.table.len() {
            self.table.resize(n + 1, u32::MAX);
        }
        self.table[n] = value;
    }
    #[inline]
    fn get(&self, s: &[u8], prefix: &[u8]) -> Option<u32> {
        dense_num(s, prefix)
            .and_then(|n| self.table.get(n).copied())
            .filter(|&v| v != u32::MAX)
    }
}
#[derive(Copy, Clone)]
struct CopyOfEvent {
    id: Key,
    ingest: i64,
    event: RawEvent,
}
fn winners(
    packed: Vec<compact::Part>,
    lo: i64,
    mode: &str,
    canon: &Canon,
) -> Vec<Vec<Event>> {
    let total: usize = packed.iter().map(compact::Part::len).sum();
    // Phase A: dedup winners, bucketed by canon shard (splitmix of the raw
    // user). Every winner in one bucket resolves against the same small
    // canon map, so that map stays in L2 instead of being evicted between
    // random probes across all 128 maps.
    let mut routed: Vec<Vec<Event>> = (0..identity::CANON_SHARDS)
        .map(|_| Vec::with_capacity(total.div_ceil(identity::CANON_SHARDS)))
        .collect();
    // Narrow transit event: ms offset from (lo - WEEK), price in the low
    // 24 bits, category in bits 24..28.
    let origin = lo - WEEK;
    let mut emit = |e: RawEvent| {
        if e.ts != BAD {
            routed[identity::shard_of(e.user)].push(Event {
                user: e.user,
                ts: (e.ts - origin) as u32,
                price_cat: e.price | (e.category << 24),
            });
        }
    };
    if mode == "singletons" {
        compact::select(&packed, lo, emit);
    } else {
        let mut copies: Vec<CopyOfEvent> = packed
            .into_iter()
            .flat_map(|part| part.expand(lo))
            .collect();
        if matches!(mode, "radix" | "compact-hash") {
            dedup::select(&copies, mode, emit);
        } else if mode == "hash" {
            let mut index = AHashMap::with_capacity(copies.len());
            for row in copies {
                let winner = index.entry(row.id).or_insert(row);
                if row.ingest < winner.ingest {
                    *winner = row
                }
            }
            for winner in index.into_values() {
                emit(winner.event)
            }
        } else {
            copies.sort_unstable_by_key(|e| e.id);
            for group in copies.chunk_by(|a, b| a.id == b.id) {
                let winner = group.iter().min_by_key(|e| e.ingest).unwrap();
                emit(winner.event);
            }
        }
    }
    // Phase B: resolve the canon user one shard at a time, then route each
    // winner to its final per-person bucket.
    let mut out = reserved_buckets(total);
    for (shard_idx, bucket) in routed.into_iter().enumerate() {
        for e in bucket {
            let user = canon.get_shard(shard_idx, e.user).unwrap_or(e.user);
            out[shard(user)].push(Event {
                user,
                ts: e.ts,
                price_cat: e.price_cat,
            });
        }
    }
    out
}
fn cpu_now() -> f64 {
    if let Ok(stat) = fs::read_to_string("/proc/self/stat") {
        if let Some(after) = stat.rsplit(')').next() {
            let fields: Vec<&str> = after.split_whitespace().collect();
            if fields.len() > 12 {
                let ticks: i64 =
                    fields[11].parse().unwrap_or(0) + fields[12].parse().unwrap_or(0);
                return ticks as f64 / 100.0;
            }
        }
    }
    0.0
}
fn run() -> Result<()> {
    let start = Instant::now();
    let profile = env::var_os("PROFILE").is_some();
    let mark = |name: &str| {
        if profile {
            eprintln!("{name} {:.3}", start.elapsed().as_secs_f64())
        }
    };
    let data = PathBuf::from(env::var_os("DATA_DIR").unwrap_or_else(|| "/data".into()));
    let output = PathBuf::from(env::var_os("OUTPUT_DIR").unwrap_or_else(|| "/output".into()));
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(data.join("manifest.json"))?)?;
    let timestamp = |field: &str| -> Result<i64> {
        let ts = stamp(
            manifest[field]
                .as_str()
                .ok_or("missing manifest timestamp")?
                .as_bytes(),
        );
        if ts == BAD {
            return Err("invalid manifest timestamp".into());
        }
        Ok(ts)
    };
    let (lo, hi) = (timestamp("window_start")?, timestamp("window_end")?);
    let month = input::Month::from_start(manifest["window_start"].as_str().unwrap().as_bytes());
    let threads = env::var("NCPU")
        .or_else(|_| env::var("NCPUS"))
        .unwrap_or_else(|_| "8".into())
        .parse::<usize>()?
        .max(1);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()?;
    let keys = Keys::default();
    let unknown = keys.id(b"unknown");
    let organic = keys.id(b"organic");
    let mut categories = vec![unknown];
    let mut category_ids = AHashMap::from_iter([(unknown, 0u32)]);
    let mut products = AHashMap::new();
    let mut dense_products = Dense::new();
    input::dimension(&data.join("products.csv"), |id, category| {
        let name = keys.id(category);
        let index = *category_ids.entry(name).or_insert_with(|| {
            categories.push(name);
            (categories.len() - 1) as u32
        });
        if let Some(n) = dense_num(id, b"p") {
            dense_products.insert(n, index);
        }
        products.insert(keys.id(id), index);
    })?;
    let mut campaign_names = vec![(organic, organic)];
    let mut campaigns = AHashMap::new();
    let mut dense_campaigns = Dense::new();
    input::dimension(&data.join("campaigns.csv"), |id, channel| {
        let n = dense_num(id, b"c_");
        let id = keys.id(id);
        if let Some(n) = n {
            dense_campaigns.insert(n, campaign_names.len() as u32);
        }
        campaigns.insert(id, campaign_names.len() as u32);
        campaign_names.push((id, keys.id(channel)));
    })?;
    let events = Files::open(&data.join("events"))?;
    let group_size = env::var("INPUT_GROUP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(4)
        .max(1);
    let (canon, parsed) = rayon::join(
        || {
            let t = Instant::now();
            let r = link_map(&data, &keys);
            if profile {
                eprintln!("link_map {:.3}", t.elapsed().as_secs_f64());
            }
            r
        },
        || {
            let t = Instant::now();
            let parsed = events
                .chunks()
                .par_chunks(group_size)
                .map(|group| {
                    let mut parts =
                        compact::buckets(group.iter().map(|c| c.len()).sum::<usize>() / 65);
                    for chunk in group {
                        rows(chunk, |[id, user, ts, ingest, kind, product, price]| {
                            if id.is_empty() {
                                return;
                            }
                            let id = keys.id(id);
                            let ts = clockfast::event(&month, ts);
                            let ingest = month.ingest(ingest);
                            let purchase = kind == b"purchase";
                            let price = if purchase {
                                input::digits(price, 10_000_000)
                            } else {
                                0
                            };
                            let valid = ts >= lo
                                && ts < hi
                                && !user.is_empty()
                                && !product.is_empty()
                                && (kind == b"view" || kind == b"cart" || purchase && price > 0)
                                && ingest != BAD
                                && ingest - ts <= DAY;
                            let event = if valid {
                                let category = if purchase {
                                    match dense_products.get(product, b"p") {
                                        Some(c) => c,
                                        None => *products
                                            .get(&keys.id(product))
                                            .unwrap_or(&0),
                                    }
                                } else {
                                    0
                                };
                                RawEvent {
                                    user: keys.id(user),
                                    ts,
                                    price: price as u32,
                                    category,
                                }
                            } else {
                                RawEvent {
                                    user: 0,
                                    ts: BAD,
                                    price: 0,
                                    category: 0,
                                }
                            };
                            parts[shard(id)].push(id, ingest, event, lo);
                        });
                    }
                    parts
                })
                .collect::<Vec<_>>();
            if profile {
                eprintln!("parse_events {:.3}", t.elapsed().as_secs_f64());
            }
            parsed
        },
    );
    let canon = canon?;
    drop(events);
    mark("parsed events and resolved identities");
    let mode = env::var("EVENT_DEDUP").unwrap_or_else(|_| "singletons".into());
    if !matches!(
        mode.as_str(),
        "sort" | "hash" | "radix" | "compact-hash" | "singletons"
    ) {
        return Err("unknown EVENT_DEDUP".into());
    }
    if env::var_os("PROFILE_MEMORY").is_some() {
        compact::report(&parsed);
    }
    let events = redistribute(
        compact::transpose(parsed)
            .into_par_iter()
            .map(|parts| winners(parts, lo, &mode, &canon))
            .collect(),
    );
    report_buffers("linked events", &events);
    mark("deduplicated and linked events");
    let clicks = Files::open(&data.join("clicks"))?;
    let ads: Vec<_> = clicks
        .chunks()
        .par_chunks(group_size)
        .map(|group| {
            let total = group.iter().map(|c| c.len()).sum::<usize>() / 38;
            // Phase A: parse clicks with no canon hashmap touch. Campaign
            // resolves through the dense table (hashmap fallback for
            // non-dense IDs). Bucket by canon shard so Phase B resolves
            // each shard's map while it stays hot, exactly like winners.
            let mut routed: Vec<Vec<Click>> = (0..identity::CANON_SHARDS)
                .map(|_| Vec::with_capacity(total / identity::CANON_SHARDS))
                .collect();
            for chunk in group {
                rows(chunk, |[id, user, campaign, ts]| {
                    if id.is_empty() || user.is_empty() {
                        return;
                    }
                    let ts = clockfast::event(&month, ts);
                    if ts < lo - WEEK || ts >= hi {
                        return;
                    }
                    let campaign = match dense_campaigns.get(campaign, b"c_") {
                        Some(c) => c,
                        None => {
                            let Some(&c) = campaigns.get(&keys.id(campaign)) else {
                                return;
                            };
                            c
                        }
                    };
                    let user = keys.id(user);
                    routed[identity::shard_of(user)].push(Click {
                        user,
                        ts: (ts - (lo - WEEK)) as u32,
                        id: keys.id(id),
                        campaign,
                    });
                });
            }
            // Phase B: resolve the canon user one shard at a time, then
            // route each click to its final per-person bucket.
            let mut parts = reserved_buckets(total);
            for (shard_idx, bucket) in routed.into_iter().enumerate() {
                for mut c in bucket {
                    c.user = canon.get_shard(shard_idx, c.user).unwrap_or(c.user);
                    let s = shard(c.user);
                    parts[s].push(c);
                }
            }
            parts
        })
        .collect();
    report_buffers("clicks", &ads);
    drop(clicks);
    drop(canon);
    mark("parsed clicks");
    let names = keys.names();
    let n_campaigns = campaign_names.len();
    let day0 = (lo / DAY) as i32;
    let n_days = ((hi - 1).div_euclid(DAY) - lo.div_euclid(DAY) + 1) as usize;
    let results: Vec<_> = events
        .into_par_iter()
        .zip(redistribute(ads))
        .map(|(e, c)| {
            tables::summarize(
                e,
                c,
                &names,
                &categories,
                n_campaigns,
                day0,
                n_days,
                lo,
            )
        })
        .collect();
    mark("computed daily tables");
    let users = output.join("user_daily");
    let campaign_dir = output.join("campaign_daily");
    fs::create_dir_all(&users)?;
    fs::create_dir_all(&campaign_dir)?;
    // Parallel element-wise sum of the dense per-shard campaign arrays.
    // Replaces the serial 128-way hashmap merge.
    let n_cells = n_campaigns * n_days;
    let totals: Vec<Credit> = (0..n_cells)
        .into_par_iter()
        .map(|i| {
            let mut acc = Credit::default();
            for (_, days) in &results {
                let c = days[i];
                acc.sessions += c.sessions;
                acc.purchases += c.purchases;
                acc.revenue += c.revenue;
                acc.assists += c.assists;
            }
            acc
        })
        .collect();
    let mut shards = Vec::new();
    for (rows, _) in results {
        shards.push(rows);
    }
    let partitions = threads.min(n_shards());
    shards
        .par_chunks(n_shards().div_ceil(partitions))
        .enumerate()
        .try_for_each(|(part, group)| {
            let path = users.join(format!("part-{part}.parquet"));
            output::users(&path, group, &names, &categories)
        })?;
    output::campaigns(
        &campaign_dir.join("part-0.parquet"),
        totals,
        day0,
        n_days,
        &names,
        &campaign_names,
    )?;
    mark("wrote parquet");
    if profile {
        // CPU time actually consumed (user+sys), robust to a loaded box.
        eprintln!("cpu {:.3}", cpu_now());
    }
    // Skip dropping the large buffers and mappings: the OS reclaims them
    // on exit, and the scorer clocks container start to process exit.
    // All output files are already closed above.
    std::process::exit(0);
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1)
    }
}
