use std::{
    collections::HashSet,
    fs,
    marker::PhantomData,
    mem,
    path::Path,
    ptr,
    slice,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};

use shared_memory::{Shmem, ShmemConf, ShmemError};
use thiserror::Error;

const READY_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const SPIN_LIMIT: usize = 10_000;

#[derive(Debug, Error)]
pub enum ChannelError {
    #[error("shared memory error: {0}")]
    SharedMemory(#[from] ShmemError),
    #[error("channel is full")]
    Full,
    #[error("channel is empty")]
    Empty,
    #[error("channel capacity must be greater than zero")]
    InvalidCapacity,
    #[error("channel capacity mismatch (expected {expected}, found {found})")]
    CapacityMismatch { expected: usize, found: usize },
    #[error("channel overwrite policy mismatch (expected overwrite={expected}, found overwrite={found})")]
    OverwriteMismatch { expected: bool, found: bool },
    #[error("element type size mismatch (expected {expected}, found {found})")]
    TypeMismatch { expected: usize, found: usize },
    #[error("producer attempted to write {requested} elements but only {available} slots were available")]
    WriteOverflow { requested: usize, available: usize },
    #[error("shared memory region was not initialized within timeout")]
    NotInitialized,
    #[error("failed to list shared memory segments: {0}")]
    ListShm(std::io::Error),
    #[error("cursor already consumed by another receiver")]
    StaleCursor,
    #[error("no pending item to pop; call next() first")]
    NoPending,
}

#[repr(C, align(64))]
struct RingHeader {
    write: AtomicU64,
    read: AtomicU64,
    capacity: u64,
    item_size: u64,
    overwrite: AtomicBool,
    ready: AtomicBool,
}

struct RingLayout {
    capacity: usize,
    item_size: usize,
    data_offset: usize,
    total_size: usize,
}

/// 共享内存通道的创建参数。
#[derive(Debug, Clone, Copy)]
pub struct ChannelConfig {
    pub capacity: usize,
    pub allow_overwrite: bool,
}

impl ChannelConfig {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            allow_overwrite: false,
        }
    }

    pub fn with_overwrite(mut self, allow: bool) -> Self {
        self.allow_overwrite = allow;
        self
    }
}

/// Receiver 附着时如何确定读取起点。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReaderStart {
    #[default]
    FromBeginning,
    FromLatest,
}

/// 描述共享内存 ring 当前的状态指标。
#[derive(Debug, Clone)]
pub struct ShmInfo {
    pub name: String,
    /// 映射总字节数（包含头部与缓冲区）
    pub total_bytes: usize,
    /// 环形缓冲区可容纳的元素个数
    pub capacity: usize,
    /// 单个元素字节数
    pub item_size: usize,
    pub allow_overwrite: bool,
    pub write_pos: u64,
    pub read_pos: u64,
    /// 当前排队的元素个数
    pub queued: usize,
    /// 当前排队的数据量（字节数）
    pub used_bytes: usize,
}

impl RingLayout {
    fn new<T>(capacity: usize) -> Result<Self, ChannelError> {
        if capacity == 0 {
            return Err(ChannelError::InvalidCapacity);
        }

        let item_size = mem::size_of::<T>();
        if item_size == 0 {
            return Err(ChannelError::TypeMismatch {
                expected: 1,
                found: 0,
            });
        }

        let align = mem::align_of::<T>().max(mem::align_of::<RingHeader>());
        let header_size = mem::size_of::<RingHeader>();
        let data_offset = align_up(header_size, align);

        let data_bytes = item_size
            .checked_mul(capacity)
            .ok_or(ChannelError::InvalidCapacity)?;
        let total_size = data_offset
            .checked_add(data_bytes)
            .ok_or(ChannelError::InvalidCapacity)?;

        Ok(Self {
            capacity,
            item_size,
            data_offset,
            total_size,
        })
    }
}

fn align_up(value: usize, align: usize) -> usize {
    let mask = align - 1;
    (value + mask) & !mask
}

fn inspect_existing_shm(os_id: &str) -> Result<ShmInfo, ChannelError> {
    let shmem = ShmemConf::new().os_id(os_id).open()?;
    let header_ptr = shmem.as_ptr() as *const RingHeader;
    let header = unsafe { &*header_ptr };

    if !header.ready.load(Ordering::Acquire) {
        return Err(ChannelError::NotInitialized);
    }

    let capacity = header.capacity as usize;
    let item_size = header.item_size as usize;
    if capacity == 0 || item_size == 0 {
        return Err(ChannelError::InvalidCapacity);
    }

    let write_pos = header.write.load(Ordering::Acquire);
    let read_pos = header.read.load(Ordering::Acquire);
    let diff = write_pos.wrapping_sub(read_pos);
    let queued = diff.min(capacity as u64) as usize;
    let used_bytes = queued.saturating_mul(item_size);
    let allow_overwrite = header.overwrite.load(Ordering::Acquire);

    let info = ShmInfo {
        name: os_id.to_string(),
        total_bytes: shmem.len(),
        capacity,
        item_size,
        allow_overwrite,
        write_pos,
        read_pos,
        queued,
        used_bytes,
    };
    Ok(info)
}

struct SharedRing<T> {
    shmem: Shmem,
    layout: RingLayout,
    _marker: PhantomData<T>,
}

impl<T> SharedRing<T> {
    fn create(name: &str, config: &ChannelConfig) -> Result<Self, ChannelError> {
        let layout = RingLayout::new::<T>(config.capacity)?;
        let shmem = ShmemConf::new()
            .os_id(name)
            .size(layout.total_size)
            .create()?;
        let ring = Self {
            shmem,
            layout,
            _marker: PhantomData,
        };
        ring.initialize_header(config.allow_overwrite);
        Ok(ring)
    }

    fn open(name: &str) -> Result<Self, ChannelError> {
        let shmem = ShmemConf::new().os_id(name).open()?;
        let ring = Self {
            layout: Self::layout_from_existing::<T>(&shmem)?,
            shmem,
            _marker: PhantomData,
        };
        ring.wait_until_ready()?;
        Ok(ring)
    }

    fn open_with_capacity(name: &str, expected_capacity: usize) -> Result<Self, ChannelError> {
        let ring = Self::open(name)?;
        let actual = ring.layout.capacity;
        if actual != expected_capacity {
            return Err(ChannelError::CapacityMismatch {
                expected: expected_capacity,
                found: actual,
            });
        }
        Ok(ring)
    }

    fn create_or_open(name: &str, config: &ChannelConfig) -> Result<(Self, bool), ChannelError> {
        match Self::create(name, config) {
            Ok(ring) => Ok((ring, true)),
            Err(ChannelError::SharedMemory(ShmemError::MappingIdExists)) => {
                let ring = Self::open_with_capacity(name, config.capacity)?;
                let existing = ring.header().overwrite.load(Ordering::Acquire);
                if existing != config.allow_overwrite {
                    return Err(ChannelError::OverwriteMismatch {
                        expected: config.allow_overwrite,
                        found: existing,
                    });
                }
                Ok((ring, false))
            }
            Err(err) => Err(err),
        }
    }

    fn base_ptr(&self) -> *mut u8 {
        self.shmem.as_ptr()
    }

    fn header(&self) -> &RingHeader {
        unsafe { &*(self.base_ptr() as *const RingHeader) }
    }

    fn header_mut(&self) -> &mut RingHeader {
        unsafe { &mut *(self.base_ptr() as *mut RingHeader) }
    }

    fn data_ptr(&self) -> *mut T {
        unsafe { self.base_ptr().add(self.layout.data_offset) as *mut T }
    }

    fn slot_ptr(&self, index: usize) -> *mut T {
        unsafe { self.data_ptr().add(index) }
    }

    fn len(&self) -> usize {
        let header = self.header();
        let write = header.write.load(Ordering::Acquire);
        let read = header.read.load(Ordering::Acquire);
        write
            .wrapping_sub(read)
            .min(self.layout.capacity as u64) as usize
    }

    fn available(&self) -> usize {
        self.layout.capacity - self.len()
    }

    fn initialize_header(&self, overwrite: bool) {
        unsafe {
            ptr::write_bytes(self.base_ptr(), 0, self.layout.total_size);
        }
        let header = self.header_mut();
        header.capacity = self.layout.capacity as u64;
        header.item_size = self.layout.item_size as u64;
        header.write.store(0, Ordering::Relaxed);
        header.read.store(0, Ordering::Relaxed);
        header.overwrite.store(overwrite, Ordering::Relaxed);
        header.ready.store(true, Ordering::Release);
    }

    fn wait_until_ready(&self) -> Result<(), ChannelError> {
        let header = self.header();
        let start = Instant::now();
        while !header.ready.load(Ordering::Acquire) {
            if start.elapsed() > READY_WAIT_TIMEOUT {
                return Err(ChannelError::NotInitialized);
            }
            thread::yield_now();
        }
        Ok(())
    }

    fn layout_from_existing<U>(shmem: &Shmem) -> Result<RingLayout, ChannelError> {
        let header = unsafe { &*(shmem.as_ptr() as *const RingHeader) };
        let start = Instant::now();
        while !header.ready.load(Ordering::Acquire) {
            if start.elapsed() > READY_WAIT_TIMEOUT {
                return Err(ChannelError::NotInitialized);
            }
            thread::yield_now();
        }
        let capacity = header.capacity as usize;
        let expected = mem::size_of::<U>();
        let found = header.item_size as usize;
        if expected != found {
            return Err(ChannelError::TypeMismatch { expected, found });
        }
        RingLayout::new::<U>(capacity)
    }
}

pub struct Sender<T> {
    ring: SharedRing<T>,
}

pub struct Receiver<T> {
    ring: SharedRing<T>,
    local_head: AtomicU64,
    last_cursor: AtomicU64,
    has_pending: AtomicBool,
}

impl<T: Copy + Send> Sender<T> {
    /// Create a brand new shared-memory ring buffer channel.
    pub fn create(name: impl AsRef<str>, capacity: usize) -> Result<Self, ChannelError> {
        Self::create_with_config(name, ChannelConfig::new(capacity))
    }

    /// Create a channel with additional configuration.
    pub fn create_with_config(
        name: impl AsRef<str>,
        config: ChannelConfig,
    ) -> Result<Self, ChannelError> {
        Ok(Self {
            ring: SharedRing::create(name.as_ref(), &config)?,
        })
    }

    /// Attach to an existing ring buffer as a producer.
    pub fn open(name: impl AsRef<str>) -> Result<Self, ChannelError> {
        Ok(Self {
            ring: SharedRing::open(name.as_ref())?,
        })
    }

    pub fn open_with_capacity(name: impl AsRef<str>, capacity: usize) -> Result<Self, ChannelError> {
        Ok(Self {
            ring: SharedRing::open_with_capacity(name.as_ref(), capacity)?,
        })
    }

    pub fn create_or_open(name: impl AsRef<str>, capacity: usize) -> Result<(Self, bool), ChannelError> {
        Self::create_or_open_with_config(name, ChannelConfig::new(capacity))
    }

    pub fn create_or_open_with_config(
        name: impl AsRef<str>,
        config: ChannelConfig,
    ) -> Result<(Self, bool), ChannelError> {
        let (ring, created) = SharedRing::create_or_open(name.as_ref(), &config)?;
        Ok((Self { ring }, created))
    }

    pub fn capacity(&self) -> usize {
        self.ring.layout.capacity
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }

    pub fn is_full(&self) -> bool {
        !self.overwrite_allowed() && self.len() >= self.capacity()
    }

    pub fn available(&self) -> usize {
        self.ring.available()
    }

    pub fn overwrite_allowed(&self) -> bool {
        self.ring.header().overwrite.load(Ordering::Acquire)
    }

    /// Busy-wait until at least one slot is available.
    pub fn block_until_writable(&self) -> Result<(), ChannelError> {
        if !self.overwrite_allowed() {
            self.wait_for(|| !self.is_full());
        }
        Ok(())
    }

    fn wait_for<F: Fn() -> bool>(&self, predicate: F) {
        let mut spins = 0usize;
        while !predicate() {
            if spins < SPIN_LIMIT {
                spins += 1;
                std::hint::spin_loop();
            } else {
                thread::sleep(Duration::from_micros(50));
            }
        }
    }

    /// Attempt to send a single value without blocking.
    pub fn try_send(&self, value: &T) -> Result<(), ChannelError> {
        match self.send_trusted(|slots| {
            if slots.is_empty() {
                0
            } else {
                slots[0] = *value;
                1
            }
        })? {
            0 => Err(ChannelError::Full),
            _ => Ok(()),
        }
    }

    /// Send a single value, spinning until space is available.
    pub fn send(&self, value: &T) -> Result<(), ChannelError> {
        self.block_until_writable()?;
        self.try_send(value)
    }

    /// Low-level API that exposes the writable slice of the ring buffer.
    /// The closure may fill at most `slots.len()` entries and returns the number of
    /// elements actually written.
    pub fn send_trusted<F>(&self, f: F) -> Result<usize, ChannelError>
    where
        F: FnOnce(&mut [T]) -> usize,
    {
        let header = self.ring.header();
        let read = header.read.load(Ordering::Acquire);
        let write = header.write.load(Ordering::Acquire);
        let capacity = self.capacity();
        let overwrite_allowed = header.overwrite.load(Ordering::Acquire);

        let used_u64 = write.wrapping_sub(read).min(capacity as u64);
        let used = used_u64 as usize;
        let available = capacity.saturating_sub(used);
        let start = (write % capacity as u64) as usize;
        let tail_space = capacity - start;

        let contiguous = if available == 0 {
            if overwrite_allowed {
                tail_space
            } else {
                return Err(ChannelError::Full);
            }
        } else {
            tail_space.min(available)
        };

        let slice = unsafe {
            slice::from_raw_parts_mut(self.ring.slot_ptr(start), contiguous)
        };
        let written = f(slice);
        if written > contiguous {
            return Err(ChannelError::WriteOverflow {
                requested: written,
                available: contiguous,
            });
        }

        let new_write = write + written as u64;
        let mut new_read = read;

        if overwrite_allowed {
            let stored = new_write.saturating_sub(new_read);
            let max_keep = capacity as u64;
            if stored > max_keep {
                let drop = stored - max_keep;
                new_read = new_read.saturating_add(drop);
            }
        }

        header.write.store(new_write, Ordering::Release);
        if overwrite_allowed && new_read != read {
            header.read.store(new_read, Ordering::Release);
        }

        Ok(written)
    }
}

impl<T: Copy + Send> Receiver<T> {
    /// Attach to an existing ring buffer as a consumer.
    pub fn open(name: impl AsRef<str>) -> Result<Self, ChannelError> {
        Self::open_with_mode(name, ReaderStart::default())
    }

    pub fn open_with_capacity(name: impl AsRef<str>, capacity: usize) -> Result<Self, ChannelError> {
        Self::open_with_capacity_and_mode(name, capacity, ReaderStart::default())
    }

    pub fn open_with_mode(
        name: impl AsRef<str>,
        start: ReaderStart,
    ) -> Result<Self, ChannelError> {
        let ring = SharedRing::open(name.as_ref())?;
        let committed = ring.header().read.load(Ordering::Acquire);
        let receiver = Self {
            ring,
            local_head: AtomicU64::new(committed),
            last_cursor: AtomicU64::new(committed),
            has_pending: AtomicBool::new(false),
        };
        receiver.apply_start_mode(start);
        Ok(receiver)
    }

    pub fn open_with_capacity_and_mode(
        name: impl AsRef<str>,
        capacity: usize,
        start: ReaderStart,
    ) -> Result<Self, ChannelError> {
        let ring = SharedRing::open_with_capacity(name.as_ref(), capacity)?;
        let committed = ring.header().read.load(Ordering::Acquire);
        let receiver = Self {
            ring,
            local_head: AtomicU64::new(committed),
            last_cursor: AtomicU64::new(committed),
            has_pending: AtomicBool::new(false),
        };
        receiver.apply_start_mode(start);
        Ok(receiver)
    }

    pub fn capacity(&self) -> usize {
        self.ring.layout.capacity
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn block_until_readable(&self) -> Result<(), ChannelError> {
        self.wait_for(|| !self.is_empty());
        Ok(())
    }

    fn wait_for<F: Fn() -> bool>(&self, predicate: F) {
        let mut spins = 0usize;
        while !predicate() {
            if spins < SPIN_LIMIT {
                spins += 1;
                std::hint::spin_loop();
            } else {
                thread::sleep(Duration::from_micros(50));
            }
        }
    }

    fn apply_start_mode(&self, start: ReaderStart) {
        let header = self.ring.header();
        match start {
            ReaderStart::FromBeginning => {
                let committed = header.read.load(Ordering::Acquire);
                self.local_head.store(committed, Ordering::Release);
                self.last_cursor.store(committed, Ordering::Release);
                self.has_pending.store(false, Ordering::Release);
            }
            ReaderStart::FromLatest => {
                let write = header.write.load(Ordering::Acquire);
                header.read.store(write, Ordering::Release);
                self.local_head.store(write, Ordering::Release);
                self.last_cursor.store(write, Ordering::Release);
                self.has_pending.store(false, Ordering::Release);
            }
        }
    }

    /// Attempt to receive a single value without blocking (SPSC 兼容接口)。
    pub fn try_recv(&self) -> Result<T, ChannelError> {
        let mut value = None;
        match self.recv_trusted(|slice| {
            if let Some(first) = slice.first() {
                value = Some(*first);
                1
            } else {
                0
            }
        }) {
            Ok(_) => value.ok_or(ChannelError::Empty),
            Err(ChannelError::StaleCursor) => Err(ChannelError::Empty),
            Err(e) => Err(e),
        }
    }

    /// Receive a single value, spinning until data is available.
    pub fn recv(&self) -> Result<T, ChannelError> {
        self.block_until_readable()?;
        self.try_recv()
    }

    /// Low-level API that exposes the readable slice of the ring buffer.
    /// The closure may consume at most `slice.len()` elements and returns the
    /// number of entries consumed.
    pub fn recv_trusted<F>(&self, f: F) -> Result<usize, ChannelError>
    where
        F: FnOnce(&[T]) -> usize,
    {
        let header = self.ring.header();
        let write = header.write.load(Ordering::Acquire);
        let read = header.read.load(Ordering::Acquire);
        if write == read {
            return Err(ChannelError::Empty);
        }

        let capacity = self.capacity();
        let available = write
            .wrapping_sub(read)
            .min(capacity as u64) as usize;
        let start = (read % capacity as u64) as usize;
        let contiguous = (self.capacity() - start).min(available);

        let slice = unsafe {
            slice::from_raw_parts(self.ring.slot_ptr(start), contiguous)
        };
        let consumed = f(slice).min(contiguous);
        if consumed == 0 {
            return Ok(0);
        }

        match header.read.compare_exchange(
            read,
            read + consumed as u64,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(consumed),
            Err(_) => Err(ChannelError::StaleCursor),
        }
    }

    /// 查看当前队列头元素但不移动读指针。
    pub fn next(&self) -> Result<T, ChannelError> {
        let capacity = self.capacity();
        let header = self.ring.header();

        loop {
            let committed = header.read.load(Ordering::Acquire);
            let write = header.write.load(Ordering::Acquire);

            let update = self.local_head.fetch_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |local| {
                    let current = local.max(committed);
                    if current >= write {
                        None
                    } else {
                        Some(current + 1)
                    }
                },
            );

            let prev = match update {
                Ok(prev) => prev,
                Err(_) => return Err(ChannelError::Empty),
            };

            let committed_now = header.read.load(Ordering::Acquire);
            let write_now = header.write.load(Ordering::Acquire);
            let target = prev.max(committed);

            if target >= write_now {
                // 没有可读数据或被并发写覆盖，重试
                continue;
            }

            if committed_now > target {
                // 该元素已被其他消费者确认，继续寻找下一条
                continue;
            }

            let index = (target % capacity as u64) as usize;
            let value = unsafe { *self.ring.slot_ptr(index) };
            self.last_cursor.store(target, Ordering::Release);
            self.has_pending.store(true, Ordering::Release);
            return Ok(value);
        }
    }

    /// 显式推进读指针；只有第一个成功调用的读取者会将元素标记为消费完成。
    pub fn pop(&self) -> Result<(), ChannelError> {
        if self
            .has_pending
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ChannelError::NoPending);
        }

        let cursor = self.last_cursor.load(Ordering::Acquire);
        let header = self.ring.header();
        match header.read.compare_exchange(
            cursor,
            cursor + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::StaleCursor),
        }
    }
}

/// Convenience helper that creates an initialized channel and attaches a receiver immediately.
pub fn create_channel<T: Copy + Send>(
    name: impl AsRef<str>,
    capacity: usize,
) -> Result<(Sender<T>, Receiver<T>), ChannelError> {
    create_channel_with_options(name, ChannelConfig::new(capacity), ReaderStart::FromBeginning)
}

pub fn create_channel_with_options<T: Copy + Send>(
    name: impl AsRef<str>,
    config: ChannelConfig,
    start: ReaderStart,
) -> Result<(Sender<T>, Receiver<T>), ChannelError> {
    let name_ref = name.as_ref();
    let (sender, _) = Sender::create_or_open_with_config(name_ref, config)?;
    let receiver = Receiver::open_with_capacity_and_mode(name_ref, config.capacity, start)?;
    Ok((sender, receiver))
}

/// 显式删除共享内存段。调用方需确保没有存活的 Sender/Receiver 再持有映射。
pub fn unlink_channel(name: impl AsRef<str>) -> Result<(), ChannelError> {
    let name_ref = name.as_ref();
    match ShmemConf::new().os_id(name_ref).open() {
        Ok(mut shmem) => {
            // 获取所有权，以便 Drop 时执行 shm_unlink
            shmem.set_owner(true);
            drop(shmem);
            Ok(())
        }
        Err(ShmemError::MapOpenFailed(_)) => Ok(()),
        Err(err) => Err(ChannelError::SharedMemory(err)),
    }
}

/// 列出系统中可见的共享内存 ring（包含容量与当前已写入的数据量）。
/// 提示：结果基于常见的 POSIX 目录，若当前平台无对应目录，返回空列表。
pub fn list_shm() -> Result<Vec<ShmInfo>, ChannelError> {
    const CANDIDATES: [&str; 3] = ["/dev/shm", "/run/shm", "/var/run/shm"];
    let mut names = HashSet::new();
    let mut last_io_err: Option<std::io::Error> = None;

    for base in CANDIDATES.iter() {
        let path = Path::new(base);
        if !path.is_dir() {
            continue;
        }
        match fs::read_dir(path) {
            Ok(read_dir) => {
                for entry in read_dir {
                    match entry {
                        Ok(dir_entry) => {
                            let name = dir_entry.file_name();
                            if let Some(name_str) = name.to_str() {
                                if !name_str.is_empty() && !name_str.starts_with('.') {
                                    names.insert(name_str.to_owned());
                                }
                            }
                        }
                        Err(err) => last_io_err = Some(err),
                    }
                }
            }
            Err(err) => last_io_err = Some(err),
        }
    }

    let mut infos = Vec::new();
    let mut last_shm_err: Option<ChannelError> = None;

    for name in names.into_iter() {
        match inspect_existing_shm(&name) {
            Ok(info) => {
                infos.push(info);
                continue;
            }
            Err(ChannelError::SharedMemory(_)) | Err(ChannelError::NotInitialized) => {}
            Err(err) => last_shm_err = Some(err),
        }

        if !name.starts_with('/') {
            let with_slash = format!("/{}", name);
            match inspect_existing_shm(&with_slash) {
                Ok(info) => {
                    infos.push(info);
                }
                Err(ChannelError::SharedMemory(_)) | Err(ChannelError::NotInitialized) => {}
                Err(err) => last_shm_err = Some(err),
            }
        }
    }

    if infos.is_empty() {
        if let Some(err) = last_shm_err {
            return Err(err);
        }
        if let Some(err) = last_io_err {
            return Err(ChannelError::ListShm(err));
        }
    } else {
        infos.sort_by(|a, b| a.name.cmp(&b.name));
    }

    Ok(infos)
}
