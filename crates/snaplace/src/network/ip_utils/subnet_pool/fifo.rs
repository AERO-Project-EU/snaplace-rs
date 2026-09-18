use std::collections::VecDeque;

use ipnet::{Ipv4Net, Ipv4Subnets};

use super::SubnetPoolError;

/// IPv4 subnet pool that recycles subnets in a FIFO way.
///
/// # Invariant for correctness
///
/// The [`Ipv4Net`] argument provided to [`Ipv4SubnetPool::give_back`] must have been returned (at
/// some point earlier) by [`Ipv4SubnetPool::get_next`].
/// Otherwise, it may be counted multiple times as free, hence future [`Ipv4SubnetPool::get_next`]
/// calls might return it this many times.
#[derive(Debug)]
pub struct Ipv4SubnetPool<const P: u8> {
    // FIFO maximizes the time between subnet reuse, which might be crucial for tap devices.
    free: VecDeque<Ipv4Net>,
    iter: Ipv4Subnets,
}

impl<const P: u8> Ipv4SubnetPool<P> {
    pub fn new(subnet: Ipv4Net) -> Result<Self, SubnetPoolError> {
        Ok(Self {
            free: Default::default(),
            iter: subnet.subnets(P)?,
        })
    }

    #[inline]
    pub fn get_next(&mut self) -> Option<Ipv4Net> {
        self.free.pop_front().or_else(|| self.iter.next())
    }

    #[inline]
    pub fn give_back(&mut self, subnet: Ipv4Net) {
        self.free.push_back(subnet);
    }
}
