use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SurfTrie {
    // Number of children per node (fits safely in u16; max 256)
    pub degrees: Vec<u16>,
    // Cumulative edge start index per node
    pub child_offsets: Vec<u32>,
    // Edge labels (one per edge)
    pub labels: Vec<u8>,
    // Mapping from edge index to child node index (BFS order)
    pub edge_to_child: Vec<u32>,
    // Bit-packed terminal flags: ith bit indicates whether node i is terminal
    pub is_terminal_bits: Vec<u8>,
}

impl SurfTrie {
    pub fn build_from_sorted(sorted: &[Vec<u8>]) -> Self {
        #[derive(Default)]
        struct Node {
            children: BTreeMap<u8, usize>,
            terminal: bool,
        }

        let mut nodes: Vec<Node> = vec![Node::default()];
        for key in sorted {
            let mut cur = 0usize;
            for &b in key {
                let next = if let Some(&child) = nodes[cur].children.get(&b) {
                    child
                } else {
                    let id = nodes.len();
                    nodes[cur].children.insert(b, id);
                    nodes.push(Node::default());
                    id
                };
                cur = next;
            }
            nodes[cur].terminal = true;
        }

        let mut degrees = Vec::with_capacity(nodes.len());
        let mut child_offsets = Vec::with_capacity(nodes.len());
        let mut labels = Vec::new();
        let mut edge_to_child = Vec::new();
        let mut is_terminal_bits = vec![0u8; (nodes.len() + 7) / 8];

        let mut queue: VecDeque<usize> = VecDeque::new();
        queue.push_back(0);
        let mut bfs_order: Vec<usize> = Vec::new();
        while let Some(u) = queue.pop_front() {
            bfs_order.push(u);
            for (_, &v) in nodes[u].children.iter() {
                queue.push_back(v);
            }
        }
        let mut bfs_index_of: HashMap<usize, usize> = HashMap::with_capacity(nodes.len());
        for (i, &orig) in bfs_order.iter().enumerate() {
            bfs_index_of.insert(orig, i);
        }

        let mut q2: VecDeque<usize> = VecDeque::new();
        q2.push_back(0);
        let mut next_edge_idx: u32 = 0;
        while let Some(u) = q2.pop_front() {
            let bi = *bfs_index_of.get(&u).unwrap();
            if nodes[u].terminal {
                let byte = bi / 8;
                let bit = bi % 8;
                is_terminal_bits[byte] |= 1u8 << bit;
            }
            child_offsets.push(next_edge_idx);
            let deg = nodes[u].children.len() as u16;
            degrees.push(deg);
            for (&lbl, &v) in &nodes[u].children {
                labels.push(lbl);
                let child_bi = *bfs_index_of.get(&v).unwrap() as u32;
                edge_to_child.push(child_bi);
                next_edge_idx += 1;
                q2.push_back(v);
            }
        }

        SurfTrie {
            degrees,
            child_offsets,
            labels,
            edge_to_child,
            is_terminal_bits,
        }
    }

    #[inline]
    pub fn child_range(&self, node_idx: usize) -> (usize, usize) {
        let start = self.child_offsets[node_idx] as usize;
        let len = self.degrees[node_idx] as usize;
        (start, start + len)
    }

    #[inline]
    pub fn is_terminal(&self, node_idx: usize) -> bool {
        let byte = node_idx / 8;
        let bit = node_idx % 8;
        if let Some(&b) = self.is_terminal_bits.get(byte) {
            (b & (1u8 << bit)) != 0
        } else {
            false
        }
    }
}
