use std::collections::{HashMap, LinkedList};

use crate::config::FrameId;

enum AccessType { Unknown, Lookup, Scan, Index }

struct LRUKNode {
  history: LinkedList<usize>,
  k: usize,
  fid: FrameId,
  is_evictable: bool
}

struct LRUKReplacer {
  node_store: HashMap<FrameId, LRUKNode>,
  current_timestamp: usize,
  curr_size: usize,
  replacer_size: usize,
  k: usize,
}

impl LRUKReplacer {
  fn evict(&mut self) -> Option<FrameId> {
    todo!()
  }

  fn record_access(&mut self, frame_id: FrameId, access_type: AccessType) {
    self.check_frame_id_valid(frame_id);

    // If the frame is new, create new node and add it to node store

    // Frame is seen before, update access history
    todo!()
  }

  fn set_evictable(&mut self, frame_id: FrameId, set_evictable: bool) {
    self.check_frame_id_valid(frame_id);

    if let Some(node) = self.node_store.get_mut(&frame_id) {
      if node.is_evictable && !set_evictable {
        self.curr_size -= 1;
      } else if !node.is_evictable && set_evictable {
        self.curr_size += 1;
      }
      node.is_evictable = set_evictable;
    }
  }

  fn remove(frame_id: FrameId) {
    todo!()
  }

  fn size() -> usize {
    todo!()
  }

  fn check_frame_id_valid(&self, frame_id: FrameId) {
    if (frame_id as usize) >= self.replacer_size {
      panic!("Invalid frame id: {}, max frame: {}", frame_id, self.replacer_size);
    }
  }
}