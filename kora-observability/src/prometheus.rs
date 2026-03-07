//! Prometheus exposition format output.
//!
//! Formats [`StatsSnapshot`] and [`CommandHistograms`] data as
//! Prometheus text exposition format, suitable for scraping by a
//! Prometheus server on an HTTP `/metrics` endpoint.

use std::fmt::Write;

use crate::histogram::CommandHistograms;
use crate::stats::StatsSnapshot;

const CMD_NAMES: [&str; 32] = [
    "get",
    "set",
    "del",
    "incr",
    "decr",
    "mget",
    "mset",
    "exists",
    "expire",
    "ttl",
    "lpush",
    "rpush",
    "lpop",
    "rpop",
    "hset",
    "hget",
    "hgetall",
    "sadd",
    "srem",
    "ping",
    "info",
    "dbsize",
    "flush",
    "scan",
    "vecset",
    "vecquery",
    "scriptcall",
    "cmd27",
    "cmd28",
    "cmd29",
    "cmd30",
    "other",
];

/// Format metrics as Prometheus exposition text.
///
/// Produces valid Prometheus text format including:
/// - `kora_commands_total` — counter per command type
/// - `kora_keys_total` — gauge for total key count
/// - `kora_memory_used_bytes` — gauge for memory usage
/// - `kora_bytes_in_total` / `kora_bytes_out_total` — counters
/// - `kora_command_duration_seconds` — histogram per command type
pub fn format_metrics(stats: &StatsSnapshot, histograms: &CommandHistograms) -> String {
    let mut out = String::with_capacity(4096);

    write_line(
        &mut out,
        "# HELP kora_commands_total Total commands processed by type.",
    );
    write_line(&mut out, "# TYPE kora_commands_total counter");
    for (i, name) in CMD_NAMES.iter().enumerate() {
        if stats.cmd_counts[i] > 0 {
            write_metric(
                &mut out,
                "kora_commands_total",
                &[("cmd", name)],
                stats.cmd_counts[i],
            );
        }
    }

    write_line(&mut out, "# HELP kora_keys_total Total number of keys.");
    write_line(&mut out, "# TYPE kora_keys_total gauge");
    write_metric(&mut out, "kora_keys_total", &[], stats.key_count);

    write_line(
        &mut out,
        "# HELP kora_memory_used_bytes Memory used in bytes.",
    );
    write_line(&mut out, "# TYPE kora_memory_used_bytes gauge");
    write_metric(&mut out, "kora_memory_used_bytes", &[], stats.memory_used);

    write_line(&mut out, "# HELP kora_bytes_in_total Total bytes received.");
    write_line(&mut out, "# TYPE kora_bytes_in_total counter");
    write_metric(&mut out, "kora_bytes_in_total", &[], stats.bytes_in);

    write_line(&mut out, "# HELP kora_bytes_out_total Total bytes sent.");
    write_line(&mut out, "# TYPE kora_bytes_out_total counter");
    write_metric(&mut out, "kora_bytes_out_total", &[], stats.bytes_out);

    write_line(
        &mut out,
        "# HELP kora_command_duration_seconds Command latency distribution.",
    );
    write_line(&mut out, "# TYPE kora_command_duration_seconds histogram");
    for (i, name) in CMD_NAMES.iter().enumerate() {
        let count = histograms.count(i);
        if count == 0 {
            continue;
        }
        let p50 = histograms.percentile(i, 50.0);
        let p90 = histograms.percentile(i, 90.0);
        let p99 = histograms.percentile(i, 99.0);
        let p999 = histograms.percentile(i, 99.9);

        for (le, val) in &[
            ("0.0001", p50),
            ("0.001", p90),
            ("0.01", p99),
            ("0.1", p999),
            ("+Inf", count),
        ] {
            let _ = writeln!(
                out,
                "kora_command_duration_seconds_bucket{{cmd=\"{}\",le=\"{}\"}} {}",
                name, le, val
            );
        }
        let sum_ns = stats.cmd_durations_ns[i];
        let _ = writeln!(
            out,
            "kora_command_duration_seconds_sum{{cmd=\"{}\"}} {:.6}",
            name,
            sum_ns as f64 / 1_000_000_000.0
        );
        let _ = writeln!(
            out,
            "kora_command_duration_seconds_count{{cmd=\"{}\"}} {}",
            name, count
        );
    }

    out
}

fn write_line(out: &mut String, line: &str) {
    out.push_str(line);
    out.push('\n');
}

fn write_metric(out: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    if labels.is_empty() {
        let _ = writeln!(out, "{} {}", name, value);
    } else {
        let label_str: String = labels
            .iter()
            .map(|(k, v)| format!("{}=\"{}\"", k, v))
            .collect::<Vec<_>>()
            .join(",");
        let _ = writeln!(out, "{}{{{}}} {}", name, label_str, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_metrics_basic() {
        let stats = StatsSnapshot {
            total_commands: 100,
            cmd_counts: {
                let mut c = [0u64; 32];
                c[0] = 50;
                c[1] = 50;
                c
            },
            cmd_durations_ns: [0; 32],
            key_count: 42,
            memory_used: 1024,
            bytes_in: 500,
            bytes_out: 600,
        };
        let histograms = CommandHistograms::new();
        let output = format_metrics(&stats, &histograms);

        assert!(output.contains("kora_commands_total{cmd=\"get\"} 50"));
        assert!(output.contains("kora_commands_total{cmd=\"set\"} 50"));
        assert!(output.contains("kora_keys_total 42"));
        assert!(output.contains("kora_memory_used_bytes 1024"));
        assert!(output.contains("kora_bytes_in_total 500"));
        assert!(output.contains("kora_bytes_out_total 600"));
    }

    #[test]
    fn test_format_metrics_with_histograms() {
        let stats = StatsSnapshot {
            total_commands: 10,
            cmd_counts: {
                let mut c = [0u64; 32];
                c[0] = 10;
                c
            },
            cmd_durations_ns: {
                let mut d = [0u64; 32];
                d[0] = 10_000_000;
                d
            },
            key_count: 5,
            memory_used: 512,
            bytes_in: 100,
            bytes_out: 200,
        };
        let histograms = CommandHistograms::new();
        for _ in 0..10 {
            histograms.record(0, 1_000_000);
        }
        let output = format_metrics(&stats, &histograms);

        assert!(output.contains("kora_command_duration_seconds_bucket{cmd=\"get\""));
        assert!(output.contains("kora_command_duration_seconds_sum{cmd=\"get\"}"));
        assert!(output.contains("kora_command_duration_seconds_count{cmd=\"get\"} 10"));
    }

    #[test]
    fn test_format_metrics_empty() {
        let stats = StatsSnapshot {
            total_commands: 0,
            cmd_counts: [0; 32],
            cmd_durations_ns: [0; 32],
            key_count: 0,
            memory_used: 0,
            bytes_in: 0,
            bytes_out: 0,
        };
        let histograms = CommandHistograms::new();
        let output = format_metrics(&stats, &histograms);

        assert!(output.contains("kora_keys_total 0"));
        assert!(!output.contains("kora_commands_total{"));
    }
}
