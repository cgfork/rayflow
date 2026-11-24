use super::{Usage, UsagePercentiles, UsageSummary};

const NS_PER_SECOND: u128 = 1_000_000_000;
const NS_PER_MILLISECOND: u128 = 1_000_000;
const MAX_MEMORY_GAP_NS: u128 = 15 * NS_PER_SECOND;

pub(super) fn summarize(mut checkpoints: Vec<Usage>) -> Option<UsageSummary> {
    checkpoints.sort_unstable_by_key(|usage| usage.sampled_at);
    let mut previous: Option<Usage> = None;
    let mut last_cpu = None;
    let mut cpu_total_ambiguous = false;
    let mut cpu_millicores = Vec::new();
    let mut memory = MemoryAggregate::new();
    let mut working_set = MemoryAggregate::new();

    for group in checkpoints.chunk_by(|a, b| a.sampled_at == b.sampled_at) {
        let (sample, cpu_conflict) = merge_same_timestamp(group);
        cpu_total_ambiguous |= cpu_conflict;
        if let Some(cpu) = sample.cpu_usage_ns {
            cpu_total_ambiguous |= last_cpu.is_some_and(|last| cpu < last);
            last_cpu = Some(cpu);
        }
        let interval = previous.as_ref().map(|prev| {
            // Groups are strictly ordered by timestamp.
            let elapsed_ns = sample.sampled_at.as_nanosecond() - prev.sampled_at.as_nanosecond();
            (prev, elapsed_ns as u128)
        });
        if let Some((prev, elapsed_ns)) = interval
            && let (Some(before), Some(after)) = (prev.cpu_usage_ns, sample.cpu_usage_ns)
            && let Some(delta_cpu) = after.checked_sub(before)
        {
            cpu_millicores.push(u128::from(delta_cpu) * 1000 / elapsed_ns);
        }
        memory.observe(
            sample.memory_usage_bytes,
            interval.and_then(|(prev, ns)| Some((prev.memory_usage_bytes?, ns))),
        );
        working_set.observe(
            sample.memory_working_set_bytes,
            interval.and_then(|(prev, ns)| Some((prev.memory_working_set_bytes?, ns))),
        );
        previous = Some(sample);
    }

    cpu_millicores.sort_unstable();
    memory.samples.sort_unstable();
    working_set.samples.sort_unstable();
    Some(UsageSummary {
        latest: previous?,
        total_cpu_usage_ns: last_cpu.filter(|_| !cpu_total_ambiguous),
        peak_cpu_millicores: cpu_millicores
            .last()
            .and_then(|&peak| u64::try_from(peak).ok()),
        cpu_millicores_percentiles: percentiles(&cpu_millicores),
        peak_memory_usage_bytes: memory.samples.last().copied(),
        memory_usage_bytes_percentiles: percentiles(&memory.samples),
        total_memory_usage_byte_seconds: memory.byte_seconds(),
        memory_observed_ms: memory.observed_ms(),
        peak_memory_working_set_bytes: working_set.samples.last().copied(),
        memory_working_set_bytes_percentiles: percentiles(&working_set.samples),
        total_memory_working_set_byte_seconds: working_set.byte_seconds(),
        memory_working_set_observed_ms: working_set.observed_ms(),
    })
}

fn percentiles<T: Copy + TryInto<u64>>(sorted: &[T]) -> Option<UsagePercentiles> {
    let n = sorted.len();
    if n == 0 {
        return None;
    }
    let at = |p: usize| {
        // Split the product so computing ceil(p * n / 100) cannot overflow.
        let rank = (n / 100) * p + ((n % 100) * p).div_ceil(100);
        sorted[rank - 1].try_into().ok()
    };
    // Wide CPU rates remain in the distribution. An unrepresentable selected
    // rank makes this percentile set unavailable.
    Some(UsagePercentiles {
        p50: at(50)?,
        p90: at(90)?,
        p95: at(95)?,
        p99: at(99)?,
    })
}

fn merge_same_timestamp(group: &[Usage]) -> (Usage, bool) {
    if let [usage] = group {
        return (usage.clone(), false);
    }
    // Repeated samples count once. A field with conflicting values at the
    // same timestamp is unavailable for that observation.
    let cpu = agreed_value(group, |usage| usage.cpu_usage_ns);
    let sample = Usage {
        sampled_at: group[0].sampled_at,
        cpu_usage_ns: cpu.ok().flatten(),
        memory_usage_bytes: agreed_value(group, |usage| usage.memory_usage_bytes)
            .ok()
            .flatten(),
        memory_working_set_bytes: agreed_value(group, |usage| usage.memory_working_set_bytes)
            .ok()
            .flatten(),
        memory_limit_bytes: agreed_value(group, |usage| usage.memory_limit_bytes)
            .ok()
            .flatten(),
    };
    (sample, cpu.is_err())
}

fn agreed_value(group: &[Usage], field: impl Fn(&Usage) -> Option<u64>) -> Result<Option<u64>, ()> {
    let mut observations = group.iter().filter_map(field);
    let first = observations.next();
    if observations.all(|value| Some(value) == first) {
        Ok(first)
    } else {
        Err(())
    }
}

struct MemoryAggregate {
    samples: Vec<u64>,
    observed_ns: u128,
    doubled_byte_ns: Option<u128>,
}

impl MemoryAggregate {
    fn new() -> Self {
        Self {
            samples: Vec::new(),
            observed_ns: 0,
            doubled_byte_ns: Some(0),
        }
    }

    fn observe(&mut self, current: Option<u64>, previous: Option<(u64, u128)>) {
        let Some(after) = current else {
            return;
        };
        self.samples.push(after);
        if let Some((before, elapsed_ns)) = previous
            && elapsed_ns <= MAX_MEMORY_GAP_NS
        {
            self.observed_ns += elapsed_ns;
            // Accumulate twice the trapezoid area to round only once.
            let area = (u128::from(before) + u128::from(after)) * elapsed_ns;
            self.doubled_byte_ns = self
                .doubled_byte_ns
                .and_then(|total| total.checked_add(area));
        }
    }

    fn byte_seconds(&self) -> Option<u64> {
        (self.observed_ns > 0)
            .then_some(self.doubled_byte_ns?)
            .and_then(|area| u64::try_from(area / (2 * NS_PER_SECOND)).ok())
    }

    fn observed_ms(&self) -> Option<u64> {
        (self.observed_ns > 0)
            .then_some(self.observed_ns / NS_PER_MILLISECOND)
            .and_then(|ms| u64::try_from(ms).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::Timestamp;

    fn checkpoint(
        seconds: i64,
        cpu: Option<u64>,
        memory: Option<u64>,
        working_set: Option<u64>,
    ) -> Usage {
        Usage {
            sampled_at: Timestamp::from_second(seconds).unwrap(),
            cpu_usage_ns: cpu,
            memory_usage_bytes: memory,
            memory_working_set_bytes: working_set,
            memory_limit_bytes: None,
        }
    }

    fn constant_percentiles(value: u64) -> Option<UsagePercentiles> {
        Some(UsagePercentiles {
            p50: value,
            p90: value,
            p95: value,
            p99: value,
        })
    }

    #[test]
    fn reports_nearest_ranks_for_cpu_rates_and_memory_samples() {
        let mut points = vec![checkpoint(0, Some(0), None, None)];
        let mut cpu = 0;
        for n in 1..=100_u64 {
            cpu += n * NS_PER_MILLISECOND as u64;
            points.push(checkpoint(n as i64, Some(cpu), Some(n * 3), Some(n * 2)));
        }
        let usage = summarize(points).unwrap();
        assert_eq!(
            usage.cpu_millicores_percentiles.unwrap(),
            UsagePercentiles {
                p50: 50,
                p90: 90,
                p95: 95,
                p99: 99
            }
        );
        assert_eq!(
            usage.memory_usage_bytes_percentiles.unwrap(),
            UsagePercentiles {
                p50: 150,
                p90: 270,
                p95: 285,
                p99: 297
            }
        );
        assert_eq!(
            usage.memory_working_set_bytes_percentiles.unwrap(),
            UsagePercentiles {
                p50: 100,
                p90: 180,
                p95: 190,
                p99: 198
            }
        );
    }

    #[test]
    fn summarizes_unsorted_uneven_intervals_without_double_counting_cpu() {
        let first = checkpoint(0, Some(2_000_000_000), Some(100), Some(80));
        let middle = checkpoint(2, Some(5_000_000_000), Some(200), Some(120));
        let last = checkpoint(5, Some(8_000_000_000), Some(50), Some(20));
        let summary = summarize(vec![last.clone(), middle, first, last.clone()]).unwrap();
        assert_eq!(summary.latest, last);
        assert_eq!(summary.total_cpu_usage_ns, Some(8_000_000_000));
        assert_eq!(summary.peak_cpu_millicores, Some(1500));
        assert_eq!(summary.peak_memory_usage_bytes, Some(200));
        assert_eq!(summary.total_memory_usage_byte_seconds, Some(675));
        assert_eq!(summary.memory_observed_ms, Some(5000));
        assert_eq!(summary.peak_memory_working_set_bytes, Some(120));
        assert_eq!(summary.total_memory_working_set_byte_seconds, Some(410));
        assert_eq!(summary.memory_working_set_observed_ms, Some(5000));
        assert_eq!(
            summary.cpu_millicores_percentiles.unwrap(),
            UsagePercentiles {
                p50: 1000,
                p90: 1500,
                p95: 1500,
                p99: 1500
            }
        );
        assert_eq!(
            summary.memory_usage_bytes_percentiles.unwrap(),
            UsagePercentiles {
                p50: 100,
                p90: 200,
                p95: 200,
                p99: 200
            }
        );
        assert_eq!(
            summary.memory_working_set_bytes_percentiles.unwrap(),
            UsagePercentiles {
                p50: 80,
                p90: 120,
                p95: 120,
                p99: 120
            }
        );
    }

    #[test]
    fn omits_conflicting_values_and_memory_gaps() {
        let summary = summarize(vec![
            checkpoint(0, Some(1_000_000_000), Some(100), None),
            checkpoint(1, Some(2_000_000_000), Some(200), None),
            checkpoint(1, Some(8_000_000_000), Some(300), None),
            checkpoint(2, None, Some(500), None),
            checkpoint(3, Some(4_000_000_000), Some(100), None),
            checkpoint(30, Some(5_000_000_000), Some(200), None),
        ])
        .unwrap();
        assert_eq!(summary.total_cpu_usage_ns, None);
        assert_eq!(summary.peak_cpu_millicores, Some(37));
        assert_eq!(summary.peak_memory_usage_bytes, Some(500));
        assert_eq!(summary.total_memory_usage_byte_seconds, Some(300));
        assert_eq!(summary.memory_observed_ms, Some(1000));
        assert_eq!(summary.total_memory_working_set_byte_seconds, None);
        assert_eq!(summary.cpu_millicores_percentiles, constant_percentiles(37));
        assert_eq!(
            summary.memory_usage_bytes_percentiles.unwrap(),
            UsagePercentiles {
                p50: 100,
                p90: 500,
                p95: 500,
                p99: 500
            }
        );
        assert_eq!(summary.memory_working_set_bytes_percentiles, None);
    }

    #[test]
    fn omits_ambiguous_totals_and_unrepresentable_values() {
        let summary = summarize(vec![
            checkpoint(0, Some(9_000_000_000), None, None),
            checkpoint(1, Some(100_000_000), None, None),
            checkpoint(2, Some(600_000_000), None, None),
        ])
        .unwrap();
        assert_eq!(summary.total_cpu_usage_ns, None);
        assert_eq!(summary.peak_cpu_millicores, Some(500));
        assert_eq!(
            summary.cpu_millicores_percentiles,
            constant_percentiles(500)
        );

        let mut last = checkpoint(0, Some(u64::MAX), None, None);
        last.sampled_at = Timestamp::from_nanosecond(1).unwrap();
        let summary = summarize(vec![checkpoint(0, Some(0), None, None), last]).unwrap();
        assert_eq!(summary.total_cpu_usage_ns, Some(u64::MAX));
        assert_eq!(summary.peak_cpu_millicores, None);
        assert_eq!(summary.cpu_millicores_percentiles, None);

        let summary = summarize(vec![
            checkpoint(0, None, Some(u64::MAX), None),
            checkpoint(15, None, Some(u64::MAX), None),
        ])
        .unwrap();
        assert_eq!(summary.peak_memory_usage_bytes, Some(u64::MAX));
        assert_eq!(summary.total_memory_usage_byte_seconds, None);
        assert_eq!(summary.memory_observed_ms, Some(15_000));
    }

    #[test]
    fn keeps_overflowing_cpu_rates_in_the_ranked_population() {
        let mut checkpoints: Vec<_> = (0..=100)
            .map(|n| {
                let counter = n as u64 * (n as u64 + 1) / 2 * NS_PER_MILLISECOND as u64;
                checkpoint(n, Some(counter), None, None)
            })
            .collect();
        let mut overflowing = checkpoint(100, Some(u64::MAX), None, None);
        overflowing.sampled_at = Timestamp::from_nanosecond(100_000_000_001).unwrap();
        checkpoints.push(overflowing);
        let summary = summarize(checkpoints).unwrap();
        assert_eq!(summary.peak_cpu_millicores, None);
        assert_eq!(
            summary.cpu_millicores_percentiles.unwrap(),
            UsagePercentiles {
                p50: 51,
                p90: 91,
                p95: 96,
                p99: 100
            }
        );
    }

    #[test]
    fn distinguishes_a_valid_zero_from_no_observation() {
        let first = checkpoint(0, Some(0), Some(0), None);
        let summary = summarize(vec![first.clone()]).unwrap();
        assert_eq!(summary.cpu_millicores_percentiles, None);
        assert_eq!(
            summary.memory_usage_bytes_percentiles,
            constant_percentiles(0)
        );
        assert_eq!(summary.memory_working_set_bytes_percentiles, None);

        let second = checkpoint(1, Some(0), None, Some(0));
        let summary = summarize(vec![first, second]).unwrap();
        assert_eq!(summary.cpu_millicores_percentiles, constant_percentiles(0));
        assert_eq!(
            summary.memory_usage_bytes_percentiles,
            constant_percentiles(0)
        );
        assert_eq!(
            summary.memory_working_set_bytes_percentiles,
            constant_percentiles(0)
        );
    }

    #[test]
    fn loads_usage_saved_before_percentiles_were_added() {
        let saved = serde_json::json!({
            "sampled_at": "2026-01-01T00:00:00Z",
            "cpu_usage_ns": 2000,
            "total_cpu_usage_ns": 2000,
            "peak_memory_usage_bytes": 10,
        });
        let summary: UsageSummary = serde_json::from_value(saved.clone()).unwrap();
        assert_eq!(summary.total_cpu_usage_ns, Some(2000));
        assert_eq!(summary.cpu_millicores_percentiles, None);
        assert_eq!(summary.memory_usage_bytes_percentiles, None);
        assert_eq!(summary.memory_working_set_bytes_percentiles, None);
        assert_eq!(serde_json::to_value(summary).unwrap(), saved);
    }
}
