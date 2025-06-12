use crate::{common::config::PageId, storage::disk::disk_manager::DiskManager};

pub enum DiskRequest<'a> {
    Read {
        page_id: PageId,
        data: &'a [u8],
        // promise to call back when read is done
    },
    Write {
        page_id: PageId,
        data: &'a mut [u8],
        // promise to call back when write is done
    },
}

struct DiskScheduler<'a> {
    disk_manager: DiskManager,
    sender: std::sync::mpsc::Sender<DiskRequest<'a>>,
    receiver: std::sync::mpsc::Receiver<DiskRequest<'a>>,
    thread_handle: Option<std::thread::JoinHandle<()>>,
}

impl<'a> DiskScheduler<'a> {
    pub fn new(disk_manager: DiskManager) -> Self {
        Self {
            disk_manager: disk_manager,
            sender: todo!(),
            receiver: todo!(),
            thread_handle: todo!(),
        }
    }

    fn start_worker_thread() {
        
    }
}