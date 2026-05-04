pub mod generator;

use core::hash::Hash as _;
use core::hash::Hasher as _;
use core::sync::atomic::AtomicU64;
use core::sync::atomic::Ordering;

use rand::Rng;
use rand::distr::Distribution as _;
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
    operation_chooser: generator::Discrete<Operation>,
    key_chooser: generator::Number,
    field_chooser: generator::Number,
    scan_length_chooser: generator::Number,
    next: Pad,
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

    pub fn runner<'a>(&'a self) -> Runner<'a> {
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

        // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/workloads/CoreWorkload.java#L517
        let record_count_insert = self.insert_proportion * (self.operation_count as f32) * 2.0;
        let record_count_total = self.record_count as u64 + record_count_insert as u64;

        Runner {
            workload: self,
            operation_chooser,
            key_chooser: match self.request_distribution {
                RequestDistribution::Latest(zipfian) => {
                    generator::Number::zipfian_latest(record_count_total, zipfian)
                }
                RequestDistribution::Uniform => generator::Number::uniform(record_count_total),
                RequestDistribution::Zipfian(zipfian) => {
                    // Not actually zipfian
                    // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/workloads/CoreWorkload.java#L519
                    generator::Number::zipfian_scrambled(record_count_total, zipfian)
                }
            },
            field_chooser: generator::Number::uniform(self.field_count as u64),
            scan_length_chooser: {
                let scan_length_count = (self.max_scan_length - self.min_scan_length + 1) as u64;
                match self.scan_length_distribution {
                    ScanLengthDistribution::Uniform => {
                        generator::Number::uniform(scan_length_count)
                    }
                    ScanLengthDistribution::Zipfian(zipfian) => {
                        // Actually zipfian
                        // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/workloads/CoreWorkload.java#L538
                        generator::Number::zipfian(scan_length_count, zipfian)
                    }
                }
            },
            next: Pad::default(),
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
        self.operation_chooser.sample(rng)
    }

    #[inline]
    pub fn field_count(&self) -> usize {
        self.workload.field_count
    }

    #[inline]
    pub fn next_scan_length<R: Rng>(&mut self, rng: &mut R) -> usize {
        let offset = self.scan_length_chooser.sample(rng);
        self.workload.min_scan_length + offset as usize
    }

    #[inline]
    pub fn next_key_insert(&mut self) -> Key {
        Key::new(
            self.workload.insert_order,
            self.workload.record_count as u64 + self.next.0.fetch_add(1, Ordering::Relaxed),
        )
    }

    #[inline]
    pub fn next_key_read<R: Rng>(&mut self, rng: &mut R) -> Key {
        // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/workloads/CoreWorkload.java#L708-L720
        let bound = self.workload.record_count as u64 + self.next.0.load(Ordering::Relaxed);

        let key = loop {
            let key = match &mut self.key_chooser {
                generator::Number::ZipfianLatest(zipfian) => bound - zipfian.sample(rng),
                key_chooser => key_chooser.sample(rng),
            };

            if key < bound {
                break key;
            }
        };

        Key::new(self.workload.insert_order, key)
    }

    #[inline]
    pub fn next_field<R: Rng>(&mut self, rng: &mut R) -> u64 {
        self.field_chooser.sample(rng)
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
    Latest(f32),
    Uniform,
    Zipfian(f32),
}

#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum ScanLengthDistribution {
    Uniform,
    Zipfian(f32),
}

#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum InsertOrder {
    Ordered,
    Hashed,
}

// Align to reduce false sharing
#[repr(align(128))]
#[derive(Default)]
struct Pad(AtomicU64);
