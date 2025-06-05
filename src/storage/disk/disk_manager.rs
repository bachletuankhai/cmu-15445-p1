use std::{collections::HashMap, io::Write, os::unix::fs::OpenOptionsExt, sync::RwLock};

use crate::common::config::{PageId, BUSTUB_PAGE_SIZE, DEFAULT_DB_IO_SIZE};

pub(crate) struct DiskManager {
    inner: RwLock<DiskManagerInner>,
}

struct DiskManagerInner {
    page_capacity: usize,
    // stream to write log file
    log_io: std::fs::File,
    log_file_name: std::path::PathBuf,
    // stream to write database file
    db_io: std::fs::File,
    db_file_name: std::path::PathBuf,

    pages: HashMap<PageId, usize>,
    free_slots: Vec<usize>,

    flush_log: bool,

    // something like a future here
}

impl DiskManager {
    pub(crate) fn new(db_file: &std::path::Path) -> Self {
        let log_file_name = db_file.file_stem()
            .and_then(|stem| stem.to_str())
            .map(|stem| format!("{}.log", stem))
            .unwrap_or_else(|| "default.log".to_string());

        let log_io = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .read(true)
            .open(&log_file_name)
            .expect("Failed to open log file");

        let db_io = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .read(true)
            .open(db_file)
            .expect("Failed to open database file");

        db_io.set_len((DEFAULT_DB_IO_SIZE + 1) * BUSTUB_PAGE_SIZE).expect("Failed to set database file size");
        
        Self { inner: RwLock::new(DiskManagerInner {
            page_capacity: DEFAULT_DB_IO_SIZE as usize,
            log_io,
            log_file_name: log_file_name.into(),
            db_io,
            db_file_name: db_file.to_path_buf(),
            flush_log: false,
            pages: HashMap::new(),
            free_slots: Vec::new(),
        }) }
    }

    fn shut_down(&self) {
        let mut inner = self.inner.write().unwrap();
        if inner.flush_log {
            inner.log_io.flush().expect("Failed to flush log file");
        }
        inner.db_io.flush().expect("Failed to flush database file");
    }

    pub(crate) fn write_page(&self, page_id: PageId, data: &[u8]) {
        let mut inner = self.inner.write().unwrap();
        
    }
}

impl Drop for DiskManager {
    fn drop(&mut self) {
        self.shut_down();
    }
}