use crate::input::{DAY, Key, Names, WEEK};
use ahash::AHashMap;

/// Sliding 7-day click window for the fused visit (stacking).
/// For the current person, `cnt[c]` (valid when `epoch[c] == cur`) is the
/// number of that campaign's clicks with timestamps in the window ending at
/// the current session start; `active` lists the campaigns with `cnt > 0`,
/// maintained by swap-remove via `pos`, so a session start credits exactly
/// the campaigns with a valid click in [start - 7d, start]. Allocated once
/// per shard, reused across persons with no clearing.
struct AssistState {
    cnt: Vec<u32>,
    epoch: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u32>,
    cur: u32,
}
impl AssistState {
    fn new(n_campaigns: usize) -> Self {
        Self {
            cnt: vec![0; n_campaigns],
            epoch: vec![0; n_campaigns],
            pos: vec![0; n_campaigns],
            active: Vec::new(),
            cur: 0,
        }
    }
    #[inline]
    fn begin_person(&mut self) {
        self.cur += 1;
        self.active.clear();
    }
    #[inline]
    fn add(&mut self, camp: u32) {
        let camp = camp as usize;
        // Push when the campaign is new for this person (epoch) OR was
        // previously removed from the window (cnt == 0): in the latter case
        // epoch is still cur but the campaign is no longer in `active`.
        if self.epoch[camp] != self.cur || self.cnt[camp] == 0 {
            self.epoch[camp] = self.cur;
            self.cnt[camp] = 1;
            self.pos[camp] = self.active.len() as u32;
            self.active.push(camp as u32);
        } else {
            self.cnt[camp] += 1;
        }
    }
    #[inline]
    fn remove(&mut self, camp: u32) {
        let camp = camp as usize;
        debug_assert_eq!(self.epoch[camp], self.cur);
        debug_assert!(self.cnt[camp] > 0);
        self.cnt[camp] -= 1;
        if self.cnt[camp] == 0 {
            let at = self.pos[camp] as usize;
            let last = *self.active.last().unwrap();
            self.active.swap_remove(at);
            if last != camp as u32 {
                self.active[at] = last;
                self.pos[last as usize] = at as u32;
            }
        }
    }
}

/// Parse-time wide event: canonical user key, absolute ms timestamp,
/// price in cents, category index. Lives in Part/overflow and dedup only.
#[derive(Clone, Copy)]
pub struct RawEvent {
    pub user: Key,
    pub ts: i64,
    pub price: u32,
    pub category: u32,
}

/// Narrow transit event (16 bytes): canonical user key, ms offset from
/// (window_start - 7d), price in the low 24 bits, category in bits 24..28.
/// The parser caps price at 10_000_000 (< 2^24) and there are 20 product
/// categories plus `unknown`, so the pack is exact.
#[derive(Clone, Copy)]
pub struct Event {
    pub user: Key,
    pub ts: u32,
    pub price_cat: u32,
}
const _: () = assert!(size_of::<Event>() == 16);
impl Event {
    #[inline]
    pub fn price(&self) -> u32 {
        self.price_cat & 0xFF_FFFF
    }
    #[inline]
    pub fn category(&self) -> u32 {
        self.price_cat >> 24
    }
}

/// Final per-person record (8 bytes): ms offset from (window_start - 7d),
/// packed price/category as in Event. The dense person id is not stored:
/// the scatter writes each person's records contiguously, so the slice
/// position already implies the group.
#[derive(Clone, Copy)]
pub struct Narrow {
    pub ts: u32,
    pub price_cat: u32,
}
const _: () = assert!(size_of::<Narrow>() == 8);
impl Narrow {
    #[inline]
    pub fn price(&self) -> u32 {
        self.price_cat & 0xFF_FFFF
    }
    #[inline]
    pub fn category(&self) -> u32 {
        self.price_cat >> 24
    }
}

/// Rows the per-person sessionizer reads: a millisecond offset from the
/// shared origin plus the purchase price/category. Implemented by both the
/// 16-byte transit Event and the 12-byte Narrow record.
pub trait EventRow {
    fn ts_off(&self) -> u32;
    fn price(&self) -> u32;
    fn category(&self) -> u32;
}
impl EventRow for Event {
    fn ts_off(&self) -> u32 {
        self.ts
    }
    fn price(&self) -> u32 {
        self.price()
    }
    fn category(&self) -> u32 {
        self.category()
    }
}
impl EventRow for Narrow {
    fn ts_off(&self) -> u32 {
        self.ts
    }
    fn price(&self) -> u32 {
        self.price()
    }
    fn category(&self) -> u32 {
        self.category()
    }
}

#[derive(Clone, Copy)]
pub struct Click {
    pub user: Key,
    pub ts: u32,
    pub id: Key,
    pub campaign: u32,
}

/// Final per-person click (16 bytes): the user is only needed for grouping,
/// which the fused scatter already did, so it is dropped here.
#[derive(Clone, Copy)]
pub struct ClickNarrow {
    pub id: Key,
    pub ts: u32,
    pub campaign: u32,
}
const _: () = assert!(size_of::<ClickNarrow>() == 16);

/// Clicks the per-person sessionizer reads: a millisecond offset from the
/// shared origin plus the campaign and click id.
pub trait ClickRow {
    fn ts_off(&self) -> u32;
    fn campaign(&self) -> u32;
    fn id(&self) -> Key;
}
impl ClickRow for Click {
    fn ts_off(&self) -> u32 {
        self.ts
    }
    fn campaign(&self) -> u32 {
        self.campaign
    }
    fn id(&self) -> Key {
        self.id
    }
}
impl ClickRow for ClickNarrow {
    fn ts_off(&self) -> u32 {
        self.ts
    }
    fn campaign(&self) -> u32 {
        self.campaign
    }
    fn id(&self) -> Key {
        self.id
    }
}
pub struct UserDay {
    pub user: Key,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: Option<u32>,
}
#[derive(Default, Clone, Copy)]
pub struct Credit {
    pub sessions: i64,
    pub purchases: i64,
    pub revenue: i64,
    pub assists: i64,
}
struct Session {
    ts: i64,
    day: i32,
    revenue: i64,
    /// Winner campaign for this session (0 = organic), fixed at session
    /// start when the latest click is known; credited after the walk.
    credit: u32,
}

fn finish_row(
    mut row: UserDay,
    sums: &mut Vec<(u32, i64)>,
    users: &mut Vec<UserDay>,
    names: &Names,
    categories: &[Key],
) {
    row.top = sums
        .drain(..)
        .max_by(|(a, x), (b, y)| {
            x.cmp(y).then_with(|| {
                if names.less(categories[*a as usize], categories[*b as usize]) {
                    std::cmp::Ordering::Greater
                } else if a == b {
                    std::cmp::Ordering::Equal
                } else {
                    std::cmp::Ordering::Less
                }
            })
        })
        .map(|(c, _)| c);
    users.push(row);
}

/// Sessionize one person's events (in timestamp order) and clicks (in
/// timestamp order), appending the person's daily rows and crediting the
/// dense campaign arrays. `origin` is window_start - 7d, the shared base
/// for the u32 timestamp offsets; session timestamps here are absolute.
#[allow(clippy::too_many_arguments)]
fn process_person<E: EventRow, C: ClickRow>(
    users: &mut Vec<UserDay>,
    campaigns: &mut [Credit],
    sessions: &mut Vec<Session>,
    category_revenue: &mut Vec<(u32, i64)>,
    assist: &mut AssistState,
    names: &Names,
    categories: &[Key],
    n_days: usize,
    day0: i32,
    origin: i64,
    person: &[E],
    ads: &[C],
    user: Key,
) {
    sessions.clear();
    assist.begin_person();
    let mut previous = None;
    let mut daily: Option<UserDay> = None;
    // Session starts are in timestamp order, so the UTC day only changes
    // at midnight. Recompute the division only when crossing it.
    let mut day = 0i32;
    let mut next_midnight = i64::MIN;
    // Fused visit: the sessionize walk carries the click pointer. `next`
    // is the first click not yet admitted to the window, `win_head` the
    // first click still inside it; `latest` is the latest click at or
    // before the current session start (tie -> smallest id, as before).
    // Clicks and session starts are both in timestamp order, so both
    // pointers only move forward.
    let mut next = 0usize;
    let mut win_head = 0usize;
    let mut latest: Option<(u32, i64, Key)> = None;
    for e in person {
        let ts = origin + i64::from(e.ts_off());
        if previous.is_none_or(|last| ts - last > 1_800_000) {
            if ts >= next_midnight {
                day = ts.div_euclid(DAY) as i32;
                next_midnight = (i64::from(day) + 1) * DAY;
            }
            // Admit clicks at or before this session start, tracking the
            // latest for the winner credit.
            while next < ads.len() {
                let c = &ads[next];
                let cts = origin + i64::from(c.ts_off());
                if cts > ts {
                    break;
                }
                if latest.is_none_or(|(_, old_ts, old_id)| {
                    cts > old_ts || names.less(c.id(), old_id)
                }) {
                    latest = Some((c.campaign(), cts, c.id()));
                }
                assist.add(c.campaign());
                next += 1;
            }
            // Evict clicks older than 7 days before this session start.
            // A click assists a session iff its ts is in [start - 7d, start]
            // (the old interval-merge condition, restated as a window).
            let cutoff = ts - WEEK;
            while win_head < next {
                let cts = origin + i64::from(ads[win_head].ts_off());
                if cts >= cutoff {
                    break;
                }
                assist.remove(ads[win_head].campaign());
                win_head += 1;
            }
            // Winner: latest click within 7 days, else organic (campaign 0).
            let credit = latest
                .filter(|(_, cts, _)| ts - cts <= WEEK)
                .map_or(0, |(camp, _, _)| camp);
            // Assists: every campaign with a click in the window credits
            // this session start, in its day cell.
            let day_idx = (day - day0) as usize;
            for &camp in assist.active.iter() {
                campaigns[camp as usize * n_days + day_idx].assists += 1;
            }
            sessions.push(Session {
                ts,
                day,
                revenue: 0,
                credit,
            });
            if daily.as_ref().is_none_or(|row| row.day != day) {
                if let Some(row) = daily.take() {
                    finish_row(row, category_revenue, users, names, categories);
                }
                daily = Some(UserDay {
                    user,
                    day,
                    sessions: 0,
                    events: 0,
                    revenue: 0,
                    top: None,
                });
            }
            daily.as_mut().unwrap().sessions += 1;
        }
        previous = Some(ts);
        let row = daily.as_mut().unwrap();
        row.events += 1;
        if e.price() != 0 {
            let price = i64::from(e.price());
            row.revenue += price;
            sessions.last_mut().unwrap().revenue += price;
            if let Some((_, value)) = category_revenue
                .iter_mut()
                .find(|(c, _)| *c == e.category())
            {
                *value += price;
            } else {
                category_revenue.push((e.category(), price));
            }
        }
    }
    finish_row(daily.unwrap(), category_revenue, users, names, categories);

    // Winner credits were fixed at each session start (revenue is only
    // known now that the walk is done); apply them from the sessions.
    for session in sessions.iter() {
        let credit = &mut campaigns[session.credit as usize * n_days + (session.day - day0) as usize];
        credit.sessions += 1;
        credit.revenue += session.revenue;
        credit.purchases += i64::from(session.revenue != 0);
    }
}

fn gather<T>(parts: Vec<Vec<T>>) -> Vec<T> {
    let mut items = Vec::with_capacity(parts.iter().map(Vec::len).sum());
    for part in parts {
        items.extend(part);
    }
    items
}

pub fn summarize(
    event_parts: Vec<Vec<Event>>,
    click_parts: Vec<Vec<Click>>,
    names: &Names,
    categories: &[Key],
    n_campaigns: usize,
    day0: i32,
    n_days: usize,
    lo: i64,
) -> (Vec<UserDay>, Vec<Credit>) {
    let mut users = Vec::new();
    // Dense (campaign, day) array: campaign ids are 0..n_campaigns and days
    // are day0..day0+n_days, so a direct index replaces a hashmap entry()
    // per credited session and per assist. 2001 x 14 = ~900KB per shard.
    let mut campaigns = vec![Credit::default(); n_campaigns * n_days];
    let mut sessions = Vec::<Session>::new();
    let mut category_revenue = Vec::<(u32, i64)>::new();
    let mut assist = AssistState::new(n_campaigns);
    let origin = lo - WEEK;
    let mode = std::env::var("PERSON_GROUP").unwrap_or_default();
    if mode.as_str() == "sort" {
        let mut events = gather(event_parts);
        let mut clicks = gather(click_parts);
        events.sort_unstable_by_key(|e| (e.user, e.ts));
        clicks.sort_unstable_by_key(|c| (c.user, c.ts));
        let mut remaining = clicks.as_mut_slice();
        for person in events.chunk_by(|a, b| a.user == b.user) {
            let user = person[0].user;
            let earlier = remaining.partition_point(|c| c.user < user);
            remaining = &mut remaining[earlier..];
            let count = remaining.partition_point(|c| c.user == user);
            let (ads, rest) = remaining.split_at_mut(count);
            remaining = rest;
            process_person(
                &mut users,
                &mut campaigns,
                &mut sessions,
                &mut category_revenue,
                &mut assist,
                names,
                categories,
                n_days,
                day0,
                origin,
                person,
                ads,
                user,
            );
        }
    } else if mode.as_str() == "dense" {
        let events = gather(event_parts);
        let clicks = gather(click_parts);
        dense_people(events, clicks, |person, ads| {
            process_person(
                &mut users,
                &mut campaigns,
                &mut sessions,
                &mut category_revenue,
                &mut assist,
                names,
                categories,
                n_days,
                day0,
                origin,
                person,
                ads,
                person[0].user,
            );
        });
    } else if mode.as_str() == "radix" {
        let events = gather(event_parts);
        let clicks = gather(click_parts);
        radix_people(events, clicks, |person, ads| {
            process_person(
                &mut users,
                &mut campaigns,
                &mut sessions,
                &mut category_revenue,
                &mut assist,
                names,
                categories,
                n_days,
                day0,
                origin,
                person,
                ads,
                person[0].user,
            );
        });
    } else {
        fused_people(event_parts, click_parts, |person, ads, user| {
            process_person(
                &mut users,
                &mut campaigns,
                &mut sessions,
                &mut category_revenue,
                &mut assist,
                names,
                categories,
                n_days,
                day0,
                origin,
                person,
                ads,
                user,
            );
        });
    }
    (users, campaigns)
}

/// Order a shard's events and clicks by (user, ts) with two stable LSD
/// radix sorts (ts offset, then user), then merge-join by user. No hash
/// map, no per-person comparison sort: the whale person costs no more
/// than any other row, and the sorts are sequential memory traffic.
/// Timestamps here are already u32 offsets from the shared origin.
fn radix_people(
    mut events: Vec<Event>,
    mut clicks: Vec<Click>,
    mut visit: impl FnMut(&[Event], &[Click]),
) {
    if events.is_empty() {
        return;
    }
    let mut aux_e = vec![events[0]; events.len()];
    crate::radix::lsd_sort(&mut events, &mut aux_e, 6, |e, p| {
        if p < 2 {
            ((e.ts >> (16 * p)) & 0xFFFF) as u16
        } else {
            ((e.user >> (16 * (p - 2))) & 0xFFFF) as u16
        }
    });
    if !clicks.is_empty() {
        let mut aux_c = vec![clicks[0]; clicks.len()];
        crate::radix::lsd_sort(&mut clicks, &mut aux_c, 6, |c, p| {
            if p < 2 {
                ((c.ts >> (16 * p)) & 0xFFFF) as u16
            } else {
                ((c.user >> (16 * (p - 2))) & 0xFFFF) as u16
            }
        });
    }
    let mut remaining = clicks.as_slice();
    for person in events.chunk_by(|a, b| a.user == b.user) {
        let user = person[0].user;
        let earlier = remaining.partition_point(|c| c.user < user);
        remaining = &remaining[earlier..];
        let count = remaining.partition_point(|c| c.user == user);
        let (ads, rest) = remaining.split_at(count);
        remaining = rest;
        visit(person, ads);
    }
}

const NO_EVENTS: u32 = u32::MAX;

/// Insertion sort for the tiny per-person slices. `sort_unstable` costs
/// ~2µs per call even for 30 elements; a straight insertion sort is faster
/// here and only runs below the threshold.
#[inline]
fn insertion_sort<T: Copy>(slice: &mut [T], key: impl Fn(&T) -> i64) {
    for i in 1..slice.len() {
        let v = slice[i];
        let vk = key(&v);
        let mut j = i;
        while j > 0 && key(&slice[j - 1]) > vk {
            slice[j] = slice[j - 1];
            j -= 1;
        }
        slice[j] = v;
    }
}

// Convert each group's count into its exclusive end in the returned array.
// The caller can iterate group spans sequentially using those ends directly.
fn scatter<T>(rows: Vec<T>, ordinals: Vec<u32>, mut counts: Vec<usize>) -> (Box<[T]>, Vec<usize>) {
    assert_eq!(rows.len(), ordinals.len());
    let mut total = 0;
    for count in &mut counts {
        let n = *count;
        *count = total;
        total += n;
    }
    let mut result = Box::<[T]>::new_uninit_slice(total);
    for (value, ordinal) in rows.into_iter().zip(ordinals) {
        if ordinal != NO_EVENTS {
            let position = &mut counts[ordinal as usize];
            result[*position].write(value);
            *position += 1;
        }
    }
    // Each included row incremented exactly one group count in the histogram.
    // Prefix sums gave the groups disjoint ranges covering 0..total and the
    // scatter initialized one successive element for each such row.
    (unsafe { result.assume_init() }, counts)
}

fn dense_people(
    events: Vec<Event>,
    clicks: Vec<Click>,
    mut visit: impl FnMut(&[Event], &[Click]),
) {
    if events.is_empty() {
        return;
    }
    // Group IDs are shard-local. Their order has no effect on either table.
    let mut ids = AHashMap::with_capacity(events.len() / 30);
    let mut event_counts = Vec::new();
    let mut event_ordinals = Vec::with_capacity(events.len());
    for event in &events {
        let group = *ids.entry(event.user).or_insert_with(|| {
            let group = u32::try_from(event_counts.len()).expect("too many people in one shard");
            assert_ne!(group, NO_EVENTS);
            event_counts.push(0);
            group
        });
        event_counts[group as usize] += 1;
        event_ordinals.push(group);
    }
    let mut click_counts = vec![0; ids.len()];
    let mut click_ordinals = Vec::with_capacity(clicks.len());
    for click in &clicks {
        let group = ids.get(&click.user).copied().unwrap_or(NO_EVENTS);
        if group != NO_EVENTS {
            click_counts[group as usize] += 1;
        }
        click_ordinals.push(group);
    }
    drop(ids);
    let (mut events, event_ends) = scatter(events, event_ordinals, event_counts);
    let (mut clicks, click_ends) = scatter(clicks, click_ordinals, click_counts);
    let mut a = 0;
    let mut b = 0;
    for (end_a, end_b) in event_ends.into_iter().zip(click_ends) {
        let person = &mut events[a..end_a];
        let ads = &mut clicks[b..end_b];
        // Per-person sorts beat one whole-shard sort: sum of n_i log n_i
        // over small groups is far less than N log N. Insertion sort wins
        // below 64 elements, where sort_unstable's call overhead dominates.
        if person.len() >= 65536 {
            use rayon::prelude::*;
            person.par_sort_unstable_by_key(|e| e.ts);
        } else if person.len() >= 64 {
            person.sort_unstable_by_key(|e| e.ts);
        } else {
            insertion_sort(person, |e| i64::from(e.ts));
        }
        if ads.len() >= 65536 {
            use rayon::prelude::*;
            ads.par_sort_unstable_by_key(|c| c.ts);
        } else if ads.len() >= 64 {
            ads.sort_unstable_by_key(|c| c.ts);
        } else {
            insertion_sort(ads, |c| i64::from(c.ts));
        }
        visit(person, ads);
        a = end_a;
        b = end_b;
    }
}

/// Group a shard's events and clicks by person with one count-then-scatter
/// pass straight over the per-chunk parts (no gather copy), then sort each
/// person's slice by timestamp and visit it. Dense ids are assigned
/// sequentially in first-seen order, so the person order is deterministic.
/// Events land as 12-byte Narrow records, clicks as 16-byte ClickNarrow.
fn fused_people(
    event_parts: Vec<Vec<Event>>,
    click_parts: Vec<Vec<Click>>,
    mut visit: impl FnMut(&[Narrow], &[ClickNarrow], Key),
) {
    let total_events: usize = event_parts.iter().map(Vec::len).sum();
    if total_events == 0 {
        return;
    }
    let mut ids = crate::fastmap::U64Map::<u32>::with_capacity(total_events / 30, 0);
    let mut people: Vec<Key> = Vec::new();
    let mut event_counts: Vec<usize> = Vec::new();
    let mut event_ordinals: Vec<u32> = Vec::with_capacity(total_events);
    for part in &event_parts {
        for event in part {
            let (group, _) = ids.entry(event.user, || {
                let group =
                    u32::try_from(event_counts.len()).expect("too many people in one shard");
                assert_ne!(group, NO_EVENTS);
                event_counts.push(0);
                people.push(event.user);
                group
            });
            event_counts[group as usize] += 1;
            event_ordinals.push(group);
        }
    }
    let total_clicks: usize = click_parts.iter().map(Vec::len).sum();
    let mut click_counts = vec![0usize; people.len()];
    let mut click_ordinals: Vec<u32> = Vec::with_capacity(total_clicks);
    for part in &click_parts {
        for click in part {
            let group = ids.get(click.user).unwrap_or(NO_EVENTS);
            if group != NO_EVENTS {
                click_counts[group as usize] += 1;
            }
            click_ordinals.push(group);
        }
    }
    drop(ids);
    // Scatter events into per-person Narrow records. The prefix sums become
    // write cursors; after the scatter each entry is the group's end.
    let mut event_ends = event_counts;
    let mut total = 0usize;
    for count in &mut event_ends {
        let n = *count;
        *count = total;
        total += n;
    }
    let mut narrow = Box::<[Narrow]>::new_uninit_slice(total);
    let mut event_idx = 0usize;
    for part in &event_parts {
        for event in part {
            let group = event_ordinals[event_idx] as usize;
            event_idx += 1;
            let pos = &mut event_ends[group];
            narrow[*pos].write(Narrow {
                ts: event.ts,
                price_cat: event.price_cat,
            });
            *pos += 1;
        }
    }
    let mut narrow = unsafe { narrow.assume_init() };
    // Scatter clicks into per-person ClickNarrow records, dropping clicks
    // whose user has no events in this shard (they can credit nothing).
    let mut click_ends = click_counts;
    let mut click_total = 0usize;
    for count in &mut click_ends {
        let n = *count;
        *count = click_total;
        click_total += n;
    }
    let mut narrow_clicks = Box::<[ClickNarrow]>::new_uninit_slice(click_total);
    let mut click_idx = 0usize;
    for part in &click_parts {
        for click in part {
            let group = click_ordinals[click_idx];
            click_idx += 1;
            if group == NO_EVENTS {
                continue;
            }
            let group = group as usize;
            let pos = &mut click_ends[group];
            narrow_clicks[*pos].write(ClickNarrow {
                id: click.id,
                ts: click.ts,
                campaign: click.campaign,
            });
            *pos += 1;
        }
    }
    let mut narrow_clicks = unsafe { narrow_clicks.assume_init() };
    let mut a = 0usize;
    let mut b = 0usize;
    for (person_idx, user) in people.iter().enumerate() {
        let end_a = event_ends[person_idx];
        let end_b = click_ends[person_idx];
        let person = &mut narrow[a..end_a];
        let ads = &mut narrow_clicks[b..end_b];
        if person.len() >= 65536 {
            use rayon::prelude::*;
            person.par_sort_unstable_by_key(|e| e.ts);
        } else if person.len() >= 64 {
            person.sort_unstable_by_key(|e| e.ts);
        } else {
            insertion_sort(person, |e| i64::from(e.ts));
        }
        if ads.len() >= 65536 {
            use rayon::prelude::*;
            ads.par_sort_unstable_by_key(|c| c.ts);
        } else if ads.len() >= 64 {
            ads.sort_unstable_by_key(|c| c.ts);
        } else {
            insertion_sort(ads, |c| i64::from(c.ts));
        }
        visit(person, ads, *user);
        a = end_a;
        b = end_b;
    }
}
