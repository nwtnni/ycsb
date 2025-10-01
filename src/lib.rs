pub mod generator;

use core::hash::Hash as _;
use core::hash::Hasher as _;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering;

use generator::Generator as _;
use rand::Rng;
use rapidhash::RapidHasher;

pub mod workload;
pub use workload::Workload;

pub struct Loader {
    insert_order: InsertOrder,
    next_key: u64,
    last_key: u64,
}

pub struct Runner<'a> {
    workload: &'a Workload,
    acked: &'a Acknowledged,
    operation_chooser: generator::Discrete<Operation>,
    keys_total: u64,
    key_chooser: generator::Number,
    field_chooser: generator::Number,
    scan_length_chooser: generator::Number,
}

impl Workload {
    pub fn operation_count(&self) -> usize {
        self.operation_count
    }

    pub fn field_count(&self) -> usize {
        self.field_count
    }

    pub fn record_count(&self) -> usize {
        self.record_count
    }

    pub fn loader(&self, thread_count: usize, thread_id: usize) -> Loader {
        let insert_count = (self.record_count / thread_count) as u64;
        let insert_start = insert_count * thread_id as u64;
        Loader {
            insert_order: self.insert_order,
            next_key: insert_start,
            last_key: insert_start + insert_count,
        }
    }

    pub fn runner<'a>(&'a self, acked: &'a Acknowledged) -> Runner<'a> {
        let operation_chooser = generator::Discrete::new(vec![
            (Operation::Read, self.read_proportion),
            (Operation::Update, self.update_proportion),
            (Operation::Scan, self.scan_proportion),
            (Operation::Insert, self.insert_proportion),
            (
                Operation::ReadModifyWrite,
                self.read_modify_write_proportion,
            ),
            (Operation::Delete, self.delete_proportion),
        ]);

        let key_count_new = self.insert_proportion * (self.operation_count as f32) * 2.0;
        let key_count_total = self.record_count as u64 + key_count_new as u64;

        Runner {
            workload: self,
            acked,
            operation_chooser,
            keys_total: key_count_total,
            key_chooser: match self.request_distribution {
                RequestDistribution::Latest => generator::Number::zipfian(key_count_total),
                RequestDistribution::Uniform => generator::Number::uniform(key_count_total),
                RequestDistribution::Zipfian => generator::Number::zipfian(key_count_total),
            },
            field_chooser: generator::Number::uniform(self.field_count as u64),
            scan_length_chooser: {
                let scan_length_count = (self.max_scan_length - self.min_scan_length + 1) as u64;
                match self.scan_length_distribution {
                    ScanLengthDistribution::Uniform => {
                        generator::Number::uniform(scan_length_count)
                    }
                    ScanLengthDistribution::Zipfian => {
                        generator::Number::zipfian(scan_length_count)
                    }
                }
            },
        }
    }
}

impl Loader {
    #[inline]
    pub fn next_key(&mut self) -> Option<Key> {
        if self.next_key >= self.last_key {
            return None;
        }

        let key = self.next_key;
        self.next_key += 1;
        Some(Key::new(self.insert_order, key))
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Key(u64);

impl Key {
    const HASHED: u64 = 1 << 63;

    #[inline]
    fn new(order: InsertOrder, sequence: u64) -> Self {
        match order {
            InsertOrder::Ordered => Self(sequence),
            InsertOrder::Hashed => Self(sequence | Self::HASHED),
        }
    }

    #[inline]
    pub fn sequence(&self) -> u64 {
        self.0 & !Self::HASHED
    }

    #[inline]
    pub fn id(&self) -> u64 {
        match self.0 & Self::HASHED > 0 {
            false => self.sequence(),
            true => {
                let mut hasher = RapidHasher::default();
                self.sequence().hash(&mut hasher);
                hasher.finish()
            }
        }
    }
}

impl Runner<'_> {
    #[inline]
    pub fn next_operation<R: Rng>(&mut self, rng: &mut R) -> Operation {
        self.operation_chooser.next(rng)
    }

    #[inline]
    pub fn field_count(&self) -> usize {
        self.workload.field_count
    }

    #[inline]
    pub fn next_scan_length<R: Rng>(&mut self, rng: &mut R) -> usize {
        let offset = self.scan_length_chooser.next(rng);
        self.workload.min_scan_length + offset as usize
    }

    #[inline]
    pub fn next_key_insert(&mut self) -> Key {
        Key::new(
            self.workload.insert_order,
            self.workload.record_count as u64 + self.acked.next_write(),
        )
    }

    #[inline]
    pub fn next_key_read<R: Rng>(&mut self, rng: &mut R) -> Key {
        let max = self.workload.record_count as u64 + self.acked.next_read() - 1;
        let key = loop {
            let key = match self.workload.request_distribution {
                RequestDistribution::Uniform => self.key_chooser.next(rng),
                RequestDistribution::Latest => match max.checked_sub(self.key_chooser.next(rng)) {
                    Some(key) => break key,
                    None => continue,
                },
                RequestDistribution::Zipfian => {
                    let key = self.key_chooser.next(rng);
                    let mut hasher = RapidHasher::default();
                    key.hash(&mut hasher);
                    hasher.finish() % self.keys_total
                }
            };

            if key <= max {
                break key;
            }
        };

        Key::new(self.workload.insert_order, key)
    }

    #[inline]
    pub fn next_field<R: Rng>(&mut self, rng: &mut R) -> u64 {
        self.field_chooser.next(rng)
    }

    /// Only track newly inserted keys
    #[inline]
    pub fn acknowledge(&self, key: Key) {
        let Some(index) = key
            .sequence()
            .checked_sub(self.workload.record_count as u64)
        else {
            return;
        };
        self.acked.acknowledge(index);
    }

    // FIXME
    #[inline]
    pub fn next_field_length<R: Rng>(&mut self, _: &mut R) -> u64 {
        100
    }
}

#[derive(Copy, Clone, Debug)]
pub enum Operation {
    Read,
    Update,
    Scan,
    Insert,
    ReadModifyWrite,
    Delete,
}

#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum RequestDistribution {
    Latest,
    Uniform,
    Zipfian,
}

#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum ScanLengthDistribution {
    Uniform,
    Zipfian,
}

#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum InsertOrder {
    Ordered,
    Hashed,
}

#[repr(C)]
pub struct Acknowledged {
    next: AtomicU64,
    hint: AtomicU64,
    inner: [AtomicU64; 1 << 20],
}

impl Default for Acknowledged {
    fn default() -> Self {
        Self::new()
    }
}

impl Acknowledged {
    pub const fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
            hint: AtomicU64::new(0),
            inner: [const { AtomicU64::new(0) }; 1 << 20],
        }
    }

    fn next_write(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    /// Max index (non-inclusive) such that all previous indices have been acknowledged.
    fn next_read(&self) -> u64 {
        let (i, j) = self.next();
        i * 64 + j
    }

    fn acknowledge(&self, index: u64) {
        let i = index / 64;
        let j = index % 64;

        self.inner[i as usize].fetch_or(1 << j, Ordering::Relaxed);
        let (hint, _) = self.next();
        self.hint.fetch_max(hint, Ordering::Relaxed);
    }

    fn next(&self) -> (u64, u64) {
        self.inner
            .iter()
            .enumerate()
            .skip(self.hint.load(Ordering::Relaxed) as usize)
            .find_map(
                |(i, row)| match row.load(Ordering::Relaxed).trailing_ones() {
                    64 => None,
                    j => Some((i as u64, j as u64)),
                },
            )
            .expect("Full acknowledgement array")
    }
}
