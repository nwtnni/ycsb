use crate::InsertOrder;
use crate::RequestDistribution;

pub const A: Workload = Workload {
    read_proportion: 0.5,
    update_proportion: 0.5,
    ..Workload::new()
};

pub const B: Workload = Workload {
    read_proportion: 0.95,
    update_proportion: 0.05,
    ..Workload::new()
};

pub const C: Workload = Workload {
    read_proportion: 1.0,
    update_proportion: 0.0,
    ..Workload::new()
};

pub const D: Workload = Workload {
    read_proportion: 0.95,
    update_proportion: 0.0,
    insert_proportion: 0.05,
    request_distribution: RequestDistribution::Latest,
    ..Workload::new()
};

/// https://github.com/brianfrankcooper/YCSB/blob/19e885f7cb780fdded0547853f7810a150554caf/core/src/main/java/site/ycsb/workloads/CoreWorkload.java#L28-L67
#[cfg_attr(
    feature = "cartesian",
    derive(cartesian::Cartesian),
    cartesian(derive(Default))
)]
#[derive(Clone, Debug)]
#[cfg_attr(
    feature = "serde",
    derive(serde::Deserialize, serde::Serialize),
    serde(default)
)]
pub struct Workload {
    pub insert_order: InsertOrder,
    pub field_count: usize,
    pub record_count: usize,
    pub operation_count: usize,
    pub read_all_fields: bool,
    pub read_proportion: f32,
    pub update_proportion: f32,
    pub scan_proportion: f32,
    pub insert_proportion: f32,
    pub read_modify_write_proportion: f32,
    pub delete_proportion: f32,
    pub request_distribution: RequestDistribution,
}

impl Workload {
    const fn new() -> Self {
        Self {
            insert_order: InsertOrder::Hashed,
            field_count: 10,
            record_count: 0,
            operation_count: 0,
            read_all_fields: true,
            read_proportion: 0.95,
            update_proportion: 0.05,
            scan_proportion: 0.0,
            insert_proportion: 0.0,
            read_modify_write_proportion: 0.0,
            delete_proportion: 0.0,
            request_distribution: RequestDistribution::Uniform,
        }
    }
}

impl Default for Workload {
    fn default() -> Self {
        Self::new()
    }
}
