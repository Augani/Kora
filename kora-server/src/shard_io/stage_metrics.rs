use std::fmt::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use hdrhistogram::Histogram;
use parking_lot::Mutex;

const NUM_BATCH_STAGES: usize = 9;
const MIN_VALUE_NS: u64 = 1_000;
const MAX_VALUE_NS: u64 = 60_000_000_000;
const SIG_FIGS: u8 = 3;

const BATCH_STAGE_NAMES: [&str; NUM_BATCH_STAGES] = [
    "parse_dispatch",
    "route",
    "execute",
    "foreign_wait",
    "serialize",
    "socket_write",
    "remote_queue",
    "remote_execute",
    "remote_delivery",
];

#[derive(Clone, Copy, Debug)]
pub(crate) enum BatchStage {
    ParseDispatch = 0,
    Route = 1,
    Execute = 2,
    ForeignWait = 3,
    Serialize = 4,
    SocketWrite = 5,
    RemoteQueue = 6,
    RemoteExecute = 7,
    RemoteDelivery = 8,
}

impl BatchStage {
    fn as_index(self) -> usize {
        self as usize
    }

    fn name(self) -> &'static str {
        BATCH_STAGE_NAMES[self.as_index()]
    }
}

pub(crate) struct BatchStageMetrics {
    enabled: AtomicBool,
    counts: [AtomicU64; NUM_BATCH_STAGES],
    sums_ns: [AtomicU64; NUM_BATCH_STAGES],
    histograms: [Mutex<Histogram<u64>>; NUM_BATCH_STAGES],
}

impl BatchStageMetrics {
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            enabled: AtomicBool::new(enabled),
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            sums_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            histograms: std::array::from_fn(|_| {
                Mutex::new(
                    Histogram::new_with_bounds(MIN_VALUE_NS, MAX_VALUE_NS, SIG_FIGS)
                        .or_else(|_| Histogram::new(SIG_FIGS))
                        .or_else(|_| Histogram::new_with_max(MAX_VALUE_NS, SIG_FIGS))
                        .expect("at least one histogram constructor must succeed"),
                )
            }),
        }
    }

    pub(crate) fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub(crate) fn record(&self, stage: BatchStage, duration_ns: u64) {
        if !self.is_enabled() {
            return;
        }

        let index = stage.as_index();
        self.counts[index].fetch_add(1, Ordering::Relaxed);
        self.sums_ns[index].fetch_add(duration_ns, Ordering::Relaxed);

        let clamped = duration_ns.clamp(MIN_VALUE_NS, MAX_VALUE_NS);
        let mut hist = self.histograms[index].lock();
        let _ = hist.record(clamped);
    }

    pub(crate) fn count(&self, stage: BatchStage) -> u64 {
        self.counts[stage.as_index()].load(Ordering::Relaxed)
    }

    pub(crate) fn sum_ns(&self, stage: BatchStage) -> u64 {
        self.sums_ns[stage.as_index()].load(Ordering::Relaxed)
    }

    pub(crate) fn average_ns(&self, stage: BatchStage) -> u64 {
        let count = self.count(stage);
        if count == 0 {
            return 0;
        }
        self.sum_ns(stage) / count
    }

    pub(crate) fn percentile_ns(&self, stage: BatchStage, percentile: f64) -> u64 {
        let hist = self.histograms[stage.as_index()].lock();
        if hist.is_empty() {
            return 0;
        }
        hist.value_at_percentile(percentile)
    }

    pub(crate) fn stages() -> [BatchStage; NUM_BATCH_STAGES] {
        [
            BatchStage::ParseDispatch,
            BatchStage::Route,
            BatchStage::Execute,
            BatchStage::ForeignWait,
            BatchStage::Serialize,
            BatchStage::SocketWrite,
            BatchStage::RemoteQueue,
            BatchStage::RemoteExecute,
            BatchStage::RemoteDelivery,
        ]
    }
}

impl Default for BatchStageMetrics {
    fn default() -> Self {
        Self::new(false)
    }
}

pub(crate) fn format_stage_metrics_info(metrics: &BatchStageMetrics) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# StageMetrics");
    let _ = writeln!(
        out,
        "stage_metrics_enabled:{}",
        if metrics.is_enabled() { 1 } else { 0 }
    );

    for stage in BatchStageMetrics::stages() {
        let name = stage.name();
        let _ = writeln!(out, "stage_{}_count:{}", name, metrics.count(stage));
        let _ = writeln!(out, "stage_{}_avg_ns:{}", name, metrics.average_ns(stage));
        let _ = writeln!(
            out,
            "stage_{}_p50_ns:{}",
            name,
            metrics.percentile_ns(stage, 50.0)
        );
        let _ = writeln!(
            out,
            "stage_{}_p99_ns:{}",
            name,
            metrics.percentile_ns(stage, 99.0)
        );
    }

    out
}

pub(crate) fn format_stage_metrics_prometheus(metrics: &BatchStageMetrics) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "# HELP kora_connection_batch_stage_duration_seconds Connection batch stage timing summary."
    );
    let _ = writeln!(
        out,
        "# TYPE kora_connection_batch_stage_duration_seconds summary"
    );

    for stage in BatchStageMetrics::stages() {
        let name = stage.name();
        let count = metrics.count(stage);
        let p50 = metrics.percentile_ns(stage, 50.0) as f64 / 1_000_000_000.0;
        let p99 = metrics.percentile_ns(stage, 99.0) as f64 / 1_000_000_000.0;
        let sum = metrics.sum_ns(stage) as f64 / 1_000_000_000.0;

        let _ = writeln!(
            out,
            "kora_connection_batch_stage_duration_seconds{{stage=\"{}\",quantile=\"0.50\"}} {:.9}",
            name, p50
        );
        let _ = writeln!(
            out,
            "kora_connection_batch_stage_duration_seconds{{stage=\"{}\",quantile=\"0.99\"}} {:.9}",
            name, p99
        );
        let _ = writeln!(
            out,
            "kora_connection_batch_stage_duration_seconds_sum{{stage=\"{}\"}} {:.9}",
            name, sum
        );
        let _ = writeln!(
            out,
            "kora_connection_batch_stage_duration_seconds_count{{stage=\"{}\"}} {}",
            name, count
        );
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stage_metrics_info_format() {
        let metrics = BatchStageMetrics::new(true);
        metrics.record(BatchStage::Execute, 10_000);
        metrics.record(BatchStage::Execute, 20_000);

        let body = format_stage_metrics_info(&metrics);
        assert!(body.contains("# StageMetrics"));
        assert!(body.contains("stage_execute_count:2"));
        assert!(body.contains("stage_execute_avg_ns:15000"));
    }

    #[test]
    fn test_stage_metrics_prometheus_format() {
        let metrics = BatchStageMetrics::new(true);
        metrics.record(BatchStage::SocketWrite, 50_000);

        let body = format_stage_metrics_prometheus(&metrics);
        assert!(body.contains("kora_connection_batch_stage_duration_seconds"));
        assert!(body.contains("stage=\"socket_write\""));
        assert!(body.contains("_count{stage=\"socket_write\"} 1"));
    }
}
