mod input;
mod output;
mod tables;
use ahash::AHashMap;
use input::{BAD, DAY, Files, Key, Keys, WEEK, hash, rows, stamp};
use rayon::prelude::*;
use std::{
    env, fs,
    path::{Path, PathBuf},
    time::Instant,
};
use tables::{CampaignDays, Click, Event};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const SHARDS: usize = 128;

#[derive(Default)]
struct Links {
    ids: AHashMap<Key, usize>,
    parents: Vec<usize>,
    sizes: Vec<usize>,
}
impl Links {
    fn index(&mut self, id: Key) -> usize {
        let n = self.parents.len();
        *self.ids.entry(id).or_insert_with(|| {
            self.parents.push(n);
            self.sizes.push(1);
            n
        })
    }
    fn root(&mut self, mut i: usize) -> usize {
        while self.parents[i] != i {
            self.parents[i] = self.parents[self.parents[i]];
            i = self.parents[i];
        }
        i
    }
    fn join(&mut self, a: Key, b: Key) {
        let (a, b) = (self.index(a), self.index(b));
        let (mut a, mut b) = (self.root(a), self.root(b));
        if a != b {
            if self.sizes[a] < self.sizes[b] {
                std::mem::swap(&mut a, &mut b)
            }
            self.parents[b] = a;
            self.sizes[a] += self.sizes[b];
        }
    }
    fn canonical(mut self, keys: &Keys) -> AHashMap<Key, Key> {
        let names = keys.names();
        let ids = std::mem::take(&mut self.ids);
        let mut minimum = vec![0; ids.len()];
        for (&id, &idx) in &ids {
            let best = &mut minimum[self.root(idx)];
            if *best == 0 || names.less(id, *best) {
                *best = id
            }
        }
        ids.into_iter()
            .filter_map(|(id, idx)| {
                let name = minimum[self.root(idx)];
                (id != name).then_some((id, name))
            })
            .collect()
    }
}
fn link_map(data: &Path, keys: &Keys) -> Result<AHashMap<Key, Key>> {
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
    let mut links = Links::default();
    for (a, b) in edges.into_iter().flatten() {
        links.join(a, b)
    }
    Ok(links.canonical(keys))
}
fn buckets<T>() -> Vec<Vec<T>> {
    (0..SHARDS).map(|_| Vec::new()).collect()
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
fn gather<T>(parts: Vec<Vec<T>>) -> Vec<T> {
    let mut items = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for part in parts {
        items.extend(part)
    }
    items
}
fn shard(id: Key) -> usize {
    (hash(id) as usize) & (SHARDS - 1)
}
#[derive(Copy, Clone)]
struct CopyOfEvent {
    id: Key,
    ingest: i64,
    event: Event,
}
fn winners(
    mut copies: Vec<CopyOfEvent>,
    hashed: bool,
    canon: &AHashMap<Key, Key>,
) -> Vec<Vec<Event>> {
    let mut out = buckets();
    let mut emit = |mut e: Event| {
        if e.ts != BAD {
            e.user = *canon.get(&e.user).unwrap_or(&e.user);
            out[shard(e.user)].push(e);
        }
    };
    if hashed {
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
    out
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
    let threads = env::var("NCPU")
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
    input::dimension(&data.join("products.csv"), |id, category| {
        let name = keys.id(category);
        let index = *category_ids.entry(name).or_insert_with(|| {
            categories.push(name);
            (categories.len() - 1) as u32
        });
        products.insert(keys.id(id), index);
    })?;
    let mut campaign_names = vec![(organic, organic)];
    let mut campaigns = AHashMap::new();
    input::dimension(&data.join("campaigns.csv"), |id, channel| {
        let id = keys.id(id);
        campaigns.insert(id, campaign_names.len() as u32);
        campaign_names.push((id, keys.id(channel)));
    })?;
    let canon = link_map(&data, &keys)?;
    mark("dimensions and identities");
    let events = Files::open(&data.join("events"))?;
    let parsed: Vec<_> = events
        .chunks()
        .into_par_iter()
        .map(|chunk| {
            let mut parts = buckets();
            rows(chunk, |[id, user, ts, ingest, kind, product, price]| {
                if id.is_empty() {
                    return;
                }
                let id = keys.id(id);
                let ts = stamp(ts);
                let ingest = stamp(ingest);
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
                        *products.get(&keys.id(product)).unwrap_or(&0)
                    } else {
                        0
                    };
                    Event {
                        user: keys.id(user),
                        ts,
                        price: price as u32,
                        category,
                    }
                } else {
                    Event {
                        user: 0,
                        ts: BAD,
                        price: 0,
                        category: 0,
                    }
                };
                parts[shard(id)].push(CopyOfEvent { id, ingest, event });
            });
            parts
        })
        .collect();
    drop(events);
    mark("parsed events");
    let hashed = match env::var("EVENT_DEDUP").as_deref() {
        Ok("hash") => true,
        Ok("sort") | Err(_) => false,
        Ok(_) => return Err("unknown EVENT_DEDUP".into()),
    };
    let events = redistribute(
        redistribute(parsed)
            .into_par_iter()
            .map(|parts| winners(gather(parts), hashed, &canon))
            .collect(),
    );
    mark("deduplicated and linked events");
    let clicks = Files::open(&data.join("clicks"))?;
    let ads: Vec<_> = clicks
        .chunks()
        .into_par_iter()
        .map(|chunk| {
            let mut parts = buckets();
            rows(chunk, |[id, user, campaign, ts]| {
                if id.is_empty() || user.is_empty() {
                    return;
                }
                let ts = stamp(ts);
                if ts < lo - WEEK || ts >= hi {
                    return;
                }
                let Some(&campaign) = campaigns.get(&keys.id(campaign)) else {
                    return;
                };
                let user = keys.id(user);
                let user = *canon.get(&user).unwrap_or(&user);
                parts[shard(user)].push(Click {
                    user,
                    ts,
                    id: keys.id(id),
                    campaign,
                });
            });
            parts
        })
        .collect();
    drop(clicks);
    drop(canon);
    mark("parsed clicks");
    let names = keys.names();
    let results: Vec<_> = events
        .into_par_iter()
        .zip(redistribute(ads))
        .map(|(e, c)| tables::summarize(gather(e), gather(c), &names, &categories))
        .collect();
    mark("computed daily tables");
    let users = output.join("user_daily");
    let campaign_dir = output.join("campaign_daily");
    fs::create_dir_all(&users)?;
    fs::create_dir_all(&campaign_dir)?;
    let mut totals = CampaignDays::new();
    let mut shards = Vec::new();
    for (rows, days) in results {
        shards.push(rows);
        for (key, v) in days {
            let d = totals.entry(key).or_default();
            d.sessions += v.sessions;
            d.purchases += v.purchases;
            d.revenue += v.revenue;
            d.assists += v.assists;
        }
    }
    let partitions = threads.min(SHARDS);
    shards
        .par_chunks(SHARDS.div_ceil(partitions))
        .enumerate()
        .try_for_each(|(part, group)| {
            let path = users.join(format!("part-{part}.parquet"));
            output::users(&path, group, &names, &categories)
        })?;
    output::campaigns(
        &campaign_dir.join("part-0.parquet"),
        totals,
        &names,
        &campaign_names,
    )?;
    mark("wrote parquet");
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1)
    }
}
