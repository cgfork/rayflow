use crate::config::Config;
use scheduler::ResourceCapacity;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

pub struct Resources {
    system: System,
    cpu_limit: u32,
    memory_limit: Option<u64>,
    first_cpu_sample: bool,
}

impl Resources {
    pub fn new(config: &Config) -> Self {
        let mut system = System::new_with_specifics(
            RefreshKind::nothing()
                .with_cpu(CpuRefreshKind::everything())
                .with_memory(MemoryRefreshKind::everything()),
        );
        system.refresh_cpu_usage();
        system.refresh_memory();
        let cores = std::thread::available_parallelism()
            .map(|count| count.get() as u32)
            .unwrap_or(1);
        Self {
            system,
            cpu_limit: config.cpu_cores.unwrap_or(cores).min(cores),
            memory_limit: config.memory_bytes,
            first_cpu_sample: true,
        }
    }

    pub async fn sample(&mut self, reserved: ResourceCapacity) -> ResourceCapacity {
        if self.first_cpu_sample {
            tokio::time::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL).await;
            self.first_cpu_sample = false;
        }
        self.system.refresh_cpu_usage();
        self.system.refresh_memory();
        let cgroup = self.system.cgroup_limits();
        let total_memory = cgroup
            .as_ref()
            .map(|limits| limits.total_memory)
            .unwrap_or(self.system.total_memory())
            .min(self.system.total_memory());
        let available_memory = cgroup
            .as_ref()
            .map(|limits| limits.free_memory)
            .unwrap_or(self.system.available_memory())
            .min(self.system.available_memory());
        let busy = (self.system.global_cpu_usage() / 100.0).clamp(0.0, 1.0);
        let cpu_cores = ((1.0 - busy) * self.cpu_limit as f32).round() as u32;
        let available = ResourceCapacity {
            cpu_cores,
            memory_bytes: available_memory,
        };
        // The server subtracts existing claims. Add them to live availability
        // here so running processes are not charged for their memory twice.
        budget(
            available,
            reserved,
            ResourceCapacity {
                cpu_cores: self.cpu_limit,
                memory_bytes: self.memory_limit.unwrap_or(total_memory).min(total_memory),
            },
        )
    }
}

fn budget(
    available: ResourceCapacity,
    reserved: ResourceCapacity,
    limit: ResourceCapacity,
) -> ResourceCapacity {
    ResourceCapacity {
        cpu_cores: available
            .cpu_cores
            .saturating_add(reserved.cpu_cores)
            .min(limit.cpu_cores),
        memory_bytes: available
            .memory_bytes
            .saturating_add(reserved.memory_bytes)
            .min(limit.memory_bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn samples_live_memory_and_bounded_cpu_capacity() {
        let mut resources = Resources::new(&Config::default());
        let sample = resources.sample(ResourceCapacity::default()).await;
        let cpu_limit = std::thread::available_parallelism()
            .map(|count| count.get() as u32)
            .unwrap_or(1);
        assert!(sample.cpu_cores <= cpu_limit);
        assert!(sample.memory_bytes > 0);
    }

    #[test]
    fn live_availability_is_not_reduced_twice_by_reservations() {
        let available = ResourceCapacity {
            cpu_cores: 2,
            memory_bytes: 600,
        };
        let reserved = ResourceCapacity {
            cpu_cores: 1,
            memory_bytes: 400,
        };
        let limit = ResourceCapacity {
            cpu_cores: 4,
            memory_bytes: 1000,
        };
        let capacity = budget(available, reserved, limit);
        assert_eq!(capacity.cpu_cores - reserved.cpu_cores, 2);
        assert_eq!(capacity.memory_bytes - reserved.memory_bytes, 600);
        let capacity = budget(
            available,
            reserved,
            ResourceCapacity {
                cpu_cores: 2,
                memory_bytes: 700,
            },
        );
        assert_eq!(capacity.cpu_cores - reserved.cpu_cores, 1);
        assert_eq!(capacity.memory_bytes - reserved.memory_bytes, 300);
    }
}
