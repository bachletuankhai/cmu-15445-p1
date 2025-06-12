use std::{
    collections::HashMap,
    f32::consts::E,
    io::{Read, Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::RwLock,
};

use crate::common::config::{BUSTUB_PAGE_SIZE, DEFAULT_DB_IO_SIZE, PageId};

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
    // something like a future here for async log flushing
    num_flushes: u64,
    num_writes: u64,
    num_deletes: u64,
}

impl DiskManager {
    pub(crate) fn new(db_file: &std::path::Path) -> Self {
        let log_file_name = db_file
            .file_stem()
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

        db_io
            .set_len(((DEFAULT_DB_IO_SIZE + 1) * BUSTUB_PAGE_SIZE) as u64)
            .expect("Failed to set database file size");

        Self {
            inner: RwLock::new(DiskManagerInner {
                page_capacity: DEFAULT_DB_IO_SIZE as usize,
                log_io,
                log_file_name: log_file_name.into(),
                db_io,
                db_file_name: db_file.to_path_buf(),
                flush_log: false,
                pages: HashMap::new(),
                free_slots: Vec::new(),
                num_flushes: 0,
                num_writes: 0,
                num_deletes: 0,
            }),
        }
    }

    fn shut_down(&self) {
        let mut inner = self.inner.write().unwrap();
        if inner.flush_log {
            inner.log_io.flush().expect("Failed to flush log file");
        }
        inner.db_io.flush().expect("Failed to flush database file");
    }

    pub(crate) fn write_page(&self, page_id: PageId, page_data: &[u8; BUSTUB_PAGE_SIZE]) {
        let mut inner = self.inner.write().unwrap();
        inner.write_page(page_id, page_data);
    }

    pub(crate) fn read_page(&self, page_id: PageId, buffer: &mut [u8; BUSTUB_PAGE_SIZE]) {
        let mut inner = self.inner.write().unwrap();
        inner.read_page(page_id, buffer);
    }

    pub(crate) fn delete_page(&self, page_id: PageId) {
        let mut inner = self.inner.write().unwrap();
        inner.delete_page(page_id);
    }

    pub(crate) fn write_log(&self, log_data: &[u8]) {
        let mut inner = self.inner.write().unwrap();
        inner.write_log(log_data);
    }

    pub(crate) fn read_log(&self, buffer: &mut [u8], offset: u64) -> bool {
        let mut inner = self.inner.write().unwrap();
        inner.read_log(buffer, offset)
    }

    pub(crate) fn get_num_flushes(&self) -> u64 {
        let inner = self.inner.read().unwrap();
        inner.num_flushes
    }

    pub(crate) fn get_num_writes(&self) -> u64 {
        let inner = self.inner.read().unwrap();
        inner.num_writes
    }

    pub(crate) fn get_num_deletes(&self) -> u64 {
        let inner = self.inner.read().unwrap();
        inner.num_deletes
    }

    pub(crate) fn get_flush_state(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.flush_log
    }
}

impl DiskManagerInner {
    fn write_page(&mut self, page_id: PageId, page_data: &[u8; BUSTUB_PAGE_SIZE]) {
        let offset: usize = self
            .pages
            .get(&page_id)
            .map(|p| p.clone())
            .or_else(|| Some(self.allocate_page()))
            .unwrap();

        if let Err(e) = self.db_io.seek(std::io::SeekFrom::Start(offset as u64)) {
            println!("Failed to seek to page offset {}: {}", offset, e);
            return;
        }

        if let Err(e) = self
            .db_io
            .write_all(page_data)
            .and_then(|_| self.db_io.flush())
        {
            println!("Failed to write page data to offset {}: {}", offset, e);
            return;
        }
        self.num_writes += 1;
        self.pages.insert(page_id, offset);
    }

    fn read_page(&mut self, page_id: PageId, buffer: &mut [u8; BUSTUB_PAGE_SIZE]) {
        // get page offset or allocate a new page
        let offset: usize = self
            .pages
            .get(&page_id)
            .map(|p| p.clone())
            .or_else(|| Some(self.allocate_page()))
            .unwrap();

        // Check if we read beyond the file length
        let file_size = get_file_size(&self.db_file_name).unwrap_or(0);
        if offset as u64 >= file_size {
            println!(
                "Attempted to read beyond the end of the file at offset {}",
                offset
            );
            return;
        }

        if let Err(e) = self
            .db_io
            .seek(std::io::SeekFrom::Start(offset as u64))
            .and_then(|_| self.db_io.read_exact(buffer))
        {
            match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // TODO: Set only unset bytes in the buffer to zero
                    println!("I/O error: Read page {} hit the end of file at offset {}", page_id, offset);
                    buffer.fill(0);
                },
                _ => {
                    println!("Failed to read page data from offset {}: {}", offset, e);
                    return;
                }
            }
        }
    }

    fn delete_page(&mut self, page_id: PageId) {
        if let Some(offset) = self.pages.remove(&page_id) {
            self.free_slots.push(offset);
            self.num_deletes += 1;
        }
    }

    fn write_log(&mut self, log_data: &[u8]) {
        if log_data.len() == 0 {
            return;
        }

        self.flush_log = true;

        // TODO: maybe something for async log flushing
        if let Err(e) = self.log_io.write_all(log_data).and_then(|_| self.log_io.flush()) {
            println!("Failed to write log data: {}", e);
            return;
        }
        self.num_flushes += 1;
        self.flush_log = false;
    }

    fn read_log(&mut self, buffer: &mut [u8], offset: u64) -> bool {
        if buffer.len() == 0 {
            return false;
        }

        if offset as u64 >= get_file_size(&self.log_file_name).unwrap_or(0) {
            println!("Buffer size is larger than log file size, reading all available data");
            return false;
        }

        if let Err(e) = self.log_io.seek(std::io::SeekFrom::Start(offset)).and_then(|_| self.log_io.read_exact(buffer)) {
            match e.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // If we hit the end of the file, we can just return false
                    println!("I/O error: Read log of size {} hit the end of file at offset {}", buffer.len(), offset);
                    buffer.fill(0); // Fill the buffer with zeros
                    return true;
                },
                _ => {
                    println!("Failed to read log data from offset {}: {}", offset, e);
                    return false;
                }
            }
        }

        true
    }

    fn allocate_page(&mut self) -> usize {
        if !self.free_slots.is_empty() {
            return self.free_slots.pop().unwrap();
        }

        if self.pages.len() + 1 >= self.page_capacity {
            self.page_capacity *= 2;
            self.db_io
                .set_len(((self.page_capacity + 1) * BUSTUB_PAGE_SIZE) as u64)
                .expect("Failed to extend database file size");
        }
        return self.pages.len() * BUSTUB_PAGE_SIZE;
    }
}

impl Drop for DiskManager {
    fn drop(&mut self) {
        self.shut_down();
    }
}

fn get_file_size(file: &PathBuf) -> Result<u64, std::io::Error> {
    match std::fs::metadata(file) {
        Ok(metadata) => Ok(metadata.len()),
        Err(e) => Err(e),
    }
}
