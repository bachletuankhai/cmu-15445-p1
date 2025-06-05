use std::{cell::RefCell, collections::{BinaryHeap, HashMap, LinkedList}, rc::Rc};

use crate::common::config::FrameId;

pub(super) enum AccessType {
    Unknown,
    Lookup,
    Scan,
    Index,
}

struct LRUKNode {
    history: LinkedList<usize>,
    k: usize,
    fid: FrameId,
    is_evictable: bool,
}

impl LRUKNode {
    fn new(fid: FrameId, k: usize) -> Self {
        Self {
            history: LinkedList::new(),
            k: k,
            fid,
            is_evictable: false,
        }
    }

    fn get_oldest_access(&self) -> usize {
        return self.history.back().cloned().unwrap_or(0);
    }

    fn get_k_access(&self) -> usize {
        self.history.iter().nth(self.k - 1).cloned().unwrap_or(0)
    }

    fn record_access(&mut self, timestamp: usize) {
        if self.history.len() == self.k {
            self.history.pop_back();
        }
        self.history.push_front(timestamp);
    }
}

impl Ord for LRUKNode {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        return self.get_k_access().cmp(&other.get_k_access());
    }
}

impl PartialOrd for LRUKNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for LRUKNode {}

impl PartialEq for LRUKNode {
    fn eq(&self, other: &Self) -> bool {
        self.fid == other.fid
    }
}

pub(super) struct LRUKReplacer {
    pq: BinaryHeap<Rc<RefCell<LRUKNode>>>,
    node_store: HashMap<FrameId, Rc<RefCell<LRUKNode>>>,
    current_timestamp: usize,
    curr_size: usize,
    replacer_size: usize,
    k: usize,
}

impl LRUKReplacer {
    pub(super) fn evict(&mut self) -> Option<FrameId> {
        while let Some(node) = self.pq.pop() {
            let node_ref = node.borrow();
            if node_ref.is_evictable {
                return Some(node_ref.fid);
            }
        }
        None
    }

    pub(super) fn record_access(&mut self, frame_id: FrameId, access_type: AccessType) {
        self.check_frame_id_valid(frame_id);

        // If the frame is new, create new node and add it to node store
        let mut node = self
            .node_store
            .entry(frame_id)
            .or_insert_with(|| Rc::new(RefCell::new(LRUKNode::new(frame_id, self.k))))
            .borrow_mut();        

        // Frame is seen before, update access history
        node.record_access(self.current_timestamp); // TODO: How to update timestamp?
    }

    pub(super) fn set_evictable(&mut self, frame_id: FrameId, set_evictable: bool) {
        self.check_frame_id_valid(frame_id);

        if let Some(node) = self.node_store.get_mut(&frame_id) {
            let node_read = node.borrow();
            if node_read.is_evictable && !set_evictable {
                self.curr_size -= 1;
            } else if !node_read.is_evictable && set_evictable {
                self.curr_size += 1;
                self.pq.push(Rc::clone(node));
            }
            let mut node = node.borrow_mut();
            node.is_evictable = set_evictable;
        }
    }

    pub(super) fn remove(&mut self, frame_id: FrameId) {
        self.check_frame_id_valid(frame_id);

        if let Some(node) = self.node_store.get(&frame_id) {
            if node.borrow().is_evictable {
                self.curr_size -= 1;
            } else {
                panic!("Cannot remove a non-evictable frame: {}", frame_id);
            }
            self.node_store.remove(&frame_id);
            // TODO: Remove from priority queue
        }
    }

    pub(super) fn size(&self) -> usize {
        self.curr_size
    }

    fn check_frame_id_valid(&self, frame_id: FrameId) {
        if (frame_id as usize) >= self.replacer_size {
            panic!(
                "Invalid frame id: {}, max frame: {}",
                frame_id, self.replacer_size
            );
        }
    }
}
