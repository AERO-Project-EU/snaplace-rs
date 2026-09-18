use std::{cmp::Reverse, collections::HashMap, fmt::Debug};

use priority_queue::PriorityQueue;
use tracing::{error, instrument, trace, warn, Level};

use crate::{sbpool::error::CpuSetError, worker::WorkerId};

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[repr(transparent)]
pub struct Cpu(u16);

impl Cpu {
    fn new(id: u16) -> Self {
        Cpu(id)
    }

    #[inline(always)]
    pub fn as_u16(&self) -> u16 {
        self.0
    }
}

#[derive(Debug)]
pub(super) struct CpuSetManager {
    /// Min-heap tracking the number of occupants (i.e., [`Sandbox`]es) per CPU.
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    pq: PriorityQueue<Cpu, Reverse<u16>, crate::BuildHasher>,
    /// Current CPU allocations for _Active_ [`Worker`]s (presumably _Running_ [`Sandbox`]es).
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`Worker`]: crate::worker::Worker
    allocations: HashMap<WorkerId, Cpu, crate::BuildHasher>,
}

impl CpuSetManager {
    #[instrument(level = Level::TRACE)]
    pub fn new(
        cpusets: &str,
        max_concurrency: impl Into<Option<usize>> + Debug,
    ) -> Result<Self, CpuSetError> {
        const DEFAULT_OVERCOMMIT_RATIO: usize = 8;

        let mut available_cpus = parse_cpuset(cpusets)?;
        if available_cpus.is_empty() {
            available_cpus = all_allowed_cpus()?;
            warn!("Using all allowed CPUs: {available_cpus:?}");
        }

        let mut pq = PriorityQueue::with_capacity_and_hasher(
            available_cpus.len(),
            crate::BuildHasher::default(),
        );
        pq.extend(
            available_cpus
                .into_iter()
                .map(Cpu::new)
                .zip(::std::iter::repeat(Reverse(0))),
        );

        Ok(Self {
            allocations: HashMap::with_capacity_and_hasher(
                max_concurrency
                    .into()
                    .unwrap_or_else(|| DEFAULT_OVERCOMMIT_RATIO * pq.len()),
                crate::BuildHasher::default(),
            ),
            pq,
        })
    }

    /// Allocate a CPU for the provided [`WorkerId`].
    ///
    /// # Panics
    ///
    /// On internal (logical) errors related to:
    /// - the priority tracked among CPUs (i.e., the number of occupant [`Sandbox`]es)
    /// - [`WorkerId`] conflicts (assuming [`WorkerId`]s _are_ unique, these should be bugs in
    /// [`SandboxPool`])
    ///
    /// [`Sandbox`]: crate::worker::runtime::Sandbox
    /// [`SandboxPool`]: crate::sbpool::SandboxPool
    #[instrument(level = Level::TRACE, skip(self))]
    #[inline]
    pub fn alloc(&mut self, worker_id: WorkerId) -> Cpu {
        let (&cpu, &num_occupants) = self.pq.peek().expect("cpuset priority queue never empties");
        let _old_priority = self.pq.change_priority(&cpu, Reverse(num_occupants.0 + 1));
        debug_assert_eq!(Some(num_occupants), _old_priority);

        trace!(?cpu, "new_#_occupants" = ?(num_occupants.0 + 1));

        assert!(
            self.allocations.insert(worker_id, cpu).is_none(),
            "WorkerId conflict in cpuset priority queue",
        );

        cpu
    }

    /// Attempt to release the CPU that had been allocated for the provided [`WorkerId`].
    ///
    /// # Errors
    ///
    /// [`CpuSet::AllocationNotFound`] is returned when no cpuset allocation is currently being
    /// tracked for the provided [`WorkerId`].
    ///
    /// # Panics
    ///
    /// On `CpuSetManager`-internal internal BUG.
    ///
    /// [`CpuSet::AllocationNotFound`]: crate::sbpool::error::CpuSetError::AllocationNotFound
    #[instrument(level = Level::TRACE, skip(self))]
    #[inline]
    pub fn release(&mut self, worker_id: WorkerId) -> Result<(), CpuSetError> {
        let cpu = self
            .allocations
            .remove(&worker_id)
            .ok_or(CpuSetError::AllocationNotFound(worker_id))?;

        let num_occupants = self
            .pq
            .get_priority(&cpu)
            .expect("no CPU can ever be missing from the priority queue");
        trace!(?cpu, "new_#_occupants" = ?(num_occupants.0 - 1));
        let _ = self.pq.change_priority(&cpu, Reverse(num_occupants.0 - 1));

        Ok(())
    }

    pub fn all_cpus(&self) -> Vec<Cpu> {
        self.pq.iter().map(|(&cpu, _)| cpu).collect()
    }
}

#[instrument(level = Level::TRACE)]
fn all_allowed_cpus() -> Result<Vec<u16>, CpuSetError> {
    const PROC_SELF_STATUS: &str = "/proc/self/status";
    const STATUS_KEY: &str = "Cpus_allowed_list";

    // SAFETY: This should always succeed on Linux >=2.6.26

    let status = ::std::fs::read_to_string(PROC_SELF_STATUS).unwrap_or_else(|err| {
        error!(error = ?err, "failed to open {PROC_SELF_STATUS}: {err:#}");
        panic!("failed to open {PROC_SELF_STATUS}: {err:#}")
    });
    for line in status.split('\n').rev() {
        if line.trim().starts_with(STATUS_KEY) {
            let cpusets = line
                .split(':')
                .next_back()
                .expect("line should be split in two")
                .trim();
            return parse_cpuset(cpusets);
        }
    }
    unreachable!("failed to find and parse all available CPUs from '{PROC_SELF_STATUS}'")
}

#[instrument(level = Level::TRACE)]
pub fn parse_cpuset(cpusets: &str) -> Result<Vec<u16>, CpuSetError> {
    let mut ret = vec![];

    for cpuset in cpusets.split_terminator(',') {
        let cpuset = cpuset.trim();
        if cpuset.is_empty() {
            continue;
        }

        let cpus: Vec<&str> = cpuset.split('-').map(|s| s.trim()).collect();
        if cpus.len() == 1 {
            let cpu_index = cpus[0].parse().map_err(|err| CpuSetError::ParseCpuRange {
                msg: format!("failed to parse '{}'", cpus[0]).into_boxed_str(),
                source: Some(err),
            })?;
            ret.push(cpu_index);
        } else {
            let start = cpus[0]
                .parse::<u16>()
                .map_err(|err| CpuSetError::ParseCpuRange {
                    msg: format!("failed to parse '{}'", cpus[0]).into_boxed_str(),
                    source: Some(err),
                })?;
            let end = cpus[1]
                .parse::<u16>()
                .map_err(|err| CpuSetError::ParseCpuRange {
                    msg: format!("failed to parse '{}'", cpus[1]).into_boxed_str(),
                    source: Some(err),
                })?;
            if start > end {
                return Err(CpuSetError::ParseCpuRange {
                    msg: format!("(start) {start} > {end} (end)").into_boxed_str(),
                    source: None,
                });
            }
            ret.extend(start..end + 1)
        }
    }

    Ok(ret)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        time::{Duration, Instant},
    };

    use tracing::info;
    use tracing_test::traced_test;
    use uuid::Uuid;

    use crate::sbpool::cpuset::CpuSetManager;

    #[test]
    #[traced_test]
    fn cpuset01() {
        const NUM_ALLOCS: u16 = 4 * 64;

        let mut cpuset = CpuSetManager::new("", None).unwrap();
        let mut durs = Vec::with_capacity(NUM_ALLOCS as _);

        let mut assignments = (0..NUM_ALLOCS)
            .map(|_| {
                let wid = Uuid::new_v4();

                let t_start = Instant::now();
                let ret = (wid, cpuset.alloc(wid));
                durs.push(t_start.elapsed());

                ret
            })
            .collect::<HashMap<_, _, crate::BuildHasher>>();
        info!(cpuset.alloc = ?durs.iter().sum::<Duration>() / NUM_ALLOCS as _);
        durs.clear();

        assignments.drain().for_each(|(wid, _cpu)| {
            let t_start = Instant::now();
            assert!(cpuset.release(wid).is_ok());
            durs.push(t_start.elapsed());
        });
        info!(cpuset.release = ?durs.iter().sum::<Duration>() / NUM_ALLOCS as _);

        assert_eq!(
            cpuset
                .pq
                .iter()
                .map(|(_cpu, num_occ)| { num_occ.0 })
                .sum::<u16>(),
            0,
            "priority queue should be all zeroes after releasing everything",
        );
    }
}
