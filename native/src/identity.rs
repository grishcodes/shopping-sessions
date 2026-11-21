use super::input::{Key, Keys, hash};
use ahash::AHashMap;

// The canonical map is large (millions of entries at 20M) and every event
// and click does a random lookup into it. Split it by the raw user ID into
// 128 small maps, each fitting in L2, so lookups hit cache instead of DRAM.
pub const CANON_SHARDS: usize = 128;

pub struct Canon {
    shards: Vec<AHashMap<Key, Key>>,
}
impl Canon {
    #[inline]
    pub fn get(&self, id: Key) -> Option<Key> {
        self.shards[hash(id) as usize & (CANON_SHARDS - 1)]
            .get(&id)
            .copied()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.len()).sum()
    }

    #[inline]
    pub fn contains_key(&self, id: Key) -> bool {
        self.get(id).is_some()
    }
}

// Non-roots point to their dense parent index. Roots store minus their size.
fn root(parents: &mut [isize], mut i: usize) -> usize {
    while parents[i] >= 0 {
        let parent = parents[i] as usize;
        let grandparent = parents[parent];
        if grandparent < 0 {
            return parent;
        }
        parents[i] = grandparent;
        i = grandparent as usize;
    }
    i
}

fn index(
    id: Key,
    indices: &mut AHashMap<Key, Key>,
    parents: &mut Vec<isize>,
    minimum: &mut Vec<Key>,
) -> usize {
    *indices.entry(id).or_insert_with(|| {
        let i = parents.len();
        parents.push(-1);
        minimum.push(id);
        i as Key
    }) as usize
}

pub(super) fn build(edges: Vec<Vec<(Key, Key)>>, keys: &Keys) -> Canon {
    let edge_count: usize = edges.iter().map(Vec::len).sum();
    let expected_ids = edge_count + edge_count / 2;
    let mut indices = AHashMap::with_capacity(expected_ids);
    let mut parents = Vec::with_capacity(expected_ids);
    let mut minimum = Vec::with_capacity(expected_ids);
    // Every endpoint was interned before the edge blocks were handed over.
    // The dictionary may continue growing concurrently with event parsing.
    let names = keys.names();
    for (a, b) in edges.into_iter().flatten() {
        if a == b || a == 0 || b == 0 {
            continue;
        }
        let a = index(a, &mut indices, &mut parents, &mut minimum);
        let b = index(b, &mut indices, &mut parents, &mut minimum);
        let (mut a, mut b) = (root(&mut parents, a), root(&mut parents, b));
        if a == b {
            continue;
        }
        if parents[a] > parents[b] {
            std::mem::swap(&mut a, &mut b);
        }
        parents[a] += parents[b];
        parents[b] = a as isize;
        if names.less(minimum[b], minimum[a]) {
            minimum[a] = minimum[b];
        }
    }
    // Replace each provisional dense index in place. Identity mappings can
    // be omitted because the lookup at emission defaults to the input ID.
    indices.retain(|&id, value| {
        *value = minimum[root(&mut parents, *value as usize)];
        *value != id
    });
    // Split the map into cache-sized shards by the raw user ID.
    let mut shards: Vec<AHashMap<Key, Key>> = (0..CANON_SHARDS)
        .map(|_| AHashMap::new())
        .collect();
    for (id, canon) in indices {
        shards[hash(id) as usize & (CANON_SHARDS - 1)].insert(id, canon);
    }
    Canon { shards }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn preserves_bytewise_minimum_and_skips_identity_mappings() {
        let keys = Keys::default();
        let z = keys.id(b"z");
        let za = keys.id(b"za");
        let old = keys.id(b" old alias longer than nine bytes");
        let unicode = keys.id("ä".as_bytes());
        let nul = keys.id(b"\0id");
        let solo = keys.id(b" SOLO ");
        let connected = build(
            vec![
                vec![(za, old), (za, z), (old, za), (solo, solo)],
                vec![(unicode, nul), (unicode, za), (0, solo)],
            ],
            &keys,
        );
        assert_eq!(connected.len(), 4);
        for id in [z, za, old, unicode] {
            assert_eq!(connected.get(id), Some(nul));
        }
        assert!(!connected.contains_key(solo));
        assert!(!connected.contains_key(nul));
    }

    #[test]
    fn agrees_with_graph_traversal_after_reordering_and_reversing_edges() {
        let keys = Keys::default();
        let names: Vec<_> = (0..240)
            .map(|i| match i % 4 {
                0 => format!("Alias-{i}"),
                1 => format!("alias with spaces-{i}"),
                2 => format!("ä{i}"),
                _ => format!("A{i}"),
            })
            .collect();
        let ids: Vec<_> = names.iter().map(|n| keys.id(n.as_bytes())).collect();
        let mut edges = Vec::new();
        let mut graph = vec![Vec::new(); ids.len()];
        for i in 0..ids.len() {
            let neighbors = [
                i,
                (i / 17) * 17 + (3 * i + 7) % 17,
                (i / 17) * 17 + (5 * i + 1) % 17,
            ];
            for j in neighbors {
                if j < ids.len() {
                    edges.push((ids[i], ids[j]));
                    graph[i].push(j);
                    graph[j].push(i);
                }
            }
        }
        let mut expected = HashMap::new();
        let mut visited = HashSet::new();
        for i in 0..ids.len() {
            if !visited.insert(i) {
                continue;
            }
            let mut component = vec![i];
            let mut next = 0;
            while next < component.len() {
                for &j in &graph[component[next]] {
                    if visited.insert(j) {
                        component.push(j);
                    }
                }
                next += 1;
            }
            let canonical = *component
                .iter()
                .min_by_key(|&&i| names[i].as_bytes())
                .unwrap();
            for j in component {
                if j != canonical {
                    expected.insert(ids[j], ids[canonical]);
                }
            }
        }
        for round in 0..2 {
            if round != 0 {
                edges.reverse();
                for (a, b) in &mut edges {
                    std::mem::swap(a, b);
                }
            }
            let blocks = edges.chunks(19).map(|chunk| chunk.to_vec()).collect();
            let actual = build(blocks, &keys);
            assert_eq!(actual.len(), expected.len());
            for (id, canonical) in &expected {
                assert_eq!(actual.get(*id), Some(*canonical));
            }
        }
    }
}
