use std::{
    collections::{HashSet, VecDeque},
    iter::Peekable,
};

use ipnet::{Ipv4Net, Ipv4Subnets};

use super::SubnetPoolError;

/// IPv4 subnet pool that recycles subnets in a FIFO way, and also allows requesting a specific
/// subnet.
///
/// # Invariant for correctness
///
/// The [`Ipv4Net`] argument provided to [`FifoExactIpv4SubnetPool::give_back`] must have
/// been returned (at some point earlier) by [`FifoExactIpv4SubnetPool::get_next`] or
/// [`FifoExactIpv4SubnetPool::get`].
/// Otherwise, it will be double-counted as free, hence future calls of the two latter methods
/// might return it (at most) twice.
///
/// On debug builds (i.e., when `debug_assertions` is enabled),
/// [`FifoExactIpv4SubnetPool::give_back`] panics if the provided `subnet` has not been acquired
/// via [`FifoExactIpv4SubnetPool::get_next`] or [`FifoExactIpv4SubnetPool::get`].
#[derive(Debug)]
pub struct FifoExactIpv4SubnetPool<const P: u8> {
    iter: Peekable<Ipv4Subnets>,
    taken: HashSet<Ipv4Net, crate::BuildHasher>,
    // FIFO maximizes the time between subnet reuse, which might be crucial for tap devices.
    free_q: VecDeque<Ipv4Net>,                      // FIFO order
    free_set: HashSet<Ipv4Net, crate::BuildHasher>, // authoritative (wrt to inclusion) set
}

impl<const P: u8> FifoExactIpv4SubnetPool<P> {
    pub fn new(subnet: Ipv4Net) -> Result<Self, SubnetPoolError> {
        Ok(Self {
            iter: subnet.subnets(P)?.peekable(),
            free_q: Default::default(),
            free_set: Default::default(),
            taken: Default::default(),
        })
    }

    #[inline]
    pub fn get_next(&mut self) -> Option<Ipv4Net> {
        while let Some(subnet) = self.free_q.pop_front() {
            // There might be stale entries in the queue, though possibly taken by a `get()`
            if self.free_set.remove(&subnet) {
                return Some(subnet);
            }
        }
        loop {
            match self.iter.next() {
                Some(subnet) if self.taken.contains(&subnet) => continue,
                subnet => return subnet,
            }
        }
    }

    /// # Panics
    ///
    /// On debug builds (i.e., when `debug_assertions` is enabled), this method panics if the
    /// provided `subnet` has not been acquired via [`FifoExactIpv4SubnetPool::get_next`] or
    /// [`FifoExactIpv4SubnetPool::get`].
    #[inline]
    pub fn give_back(&mut self, subnet: Ipv4Net) {
        if self.taken.remove(&subnet) {
            // TODO(ckatsak): Rust edition 2024 syntax -> if let Some() && ... { ... }
            match self.iter.peek() {
                Some(next_subnet) if subnet >= *next_subnet => return,
                _ => {} // fallthrough
            }
        }
        if self.free_set.insert(subnet) {
            self.free_q.push_back(subnet);
        }
    }

    pub fn get(&mut self, subnet: Ipv4Net) -> Result<Ipv4Net, SubnetPoolError> {
        match self.iter.peek() {
            Some(next_subnet) if subnet > *next_subnet => match self.taken.insert(subnet) {
                true => Ok(subnet),
                false => Err(SubnetPoolError::SubnetNotFree(subnet)),
            },
            Some(next_subnet) if subnet == *next_subnet => {
                let _next_subnet = self.iter.next().expect("self.iter.peek().is_some()");
                debug_assert_eq!(_next_subnet, subnet);
                Ok(subnet)
            }
            None | Some(_) => {
                if self.free_set.remove(&subnet) {
                    Ok(subnet)
                } else {
                    Err(SubnetPoolError::SubnetNotFree(subnet))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use ipnet::Ipv4Net;

    use super::{FifoExactIpv4SubnetPool, SubnetPoolError};

    fn net(addr: &str) -> Ipv4Net {
        Ipv4Net::from_str(addr).expect("Failed to parse network string")
    }

    #[test]
    fn test_new_pool() {
        assert!(FifoExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).is_ok());
        assert!(FifoExactIpv4SubnetPool::<22>::new(net("10.0.0.0/24")).is_err());
    }

    #[test]
    fn test_get_next_simple_sequence_and_exhaustion() {
        let mut pool = FifoExactIpv4SubnetPool::<30>::new(net("192.168.0.0/29")).unwrap();
        assert_eq!(pool.get_next(), Some(net("192.168.0.0/30")));
        assert_eq!(pool.get_next(), Some(net("192.168.0.4/30")));
        assert_eq!(pool.get_next(), None);
    }

    #[test]
    fn test_get_next_from_free_list_is_fifo() {
        let mut pool = FifoExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let subnet_a = pool.get_next().unwrap(); // 10.0.0.0/24
        let subnet_b = pool.get_next().unwrap(); // 10.0.1.0/24

        // Give back A, then B
        pool.give_back(subnet_a);
        pool.give_back(subnet_b);

        // Should get back A first (First-In, First-Out)
        assert_eq!(pool.get_next(), Some(subnet_a));
        assert_eq!(pool.get_next(), Some(subnet_b));
    }

    #[test]
    fn test_get_next_skips_stale_entries() {
        let mut pool = FifoExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let subnet_a = pool.get_next().unwrap();
        let subnet_b = pool.get_next().unwrap();
        let _subnet_c = pool.get_next().unwrap(); // Iterator is now at .3

        // Give back A and B
        pool.give_back(subnet_a);
        pool.give_back(subnet_b);
        // At this point, queue is [A, B], set is {A, B}

        // Specifically take A, which was the first one returned.
        // This removes A from the set, but leaves it in the queue as a "stale" entry.
        pool.get(subnet_a)
            .expect("Should be able to get A specifically");
        // Now, queue is [A, B], set is {B}

        // The next call to get_next should pop A, see it's stale, discard it,
        // then pop B, see it's valid, and return it.
        assert_eq!(pool.get_next(), Some(subnet_b));

        // The free list is now empty, so the next call should get a new subnet from the iterator.
        assert_eq!(pool.get_next(), Some(net("10.0.3.0/24")));
    }

    #[test]
    fn test_get_specific_at_iterator_head() {
        let mut pool = FifoExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let expected_subnet = net("10.0.0.0/24");
        let acquired_subnet = pool
            .get(expected_subnet)
            .expect("Getting head of iterator should succeed");
        assert_eq!(acquired_subnet, expected_subnet);
        assert_eq!(pool.get_next(), Some(net("10.0.1.0/24")));
    }

    #[test]
    fn test_get_specific_ahead_of_iterator_and_skip() {
        let mut pool = FifoExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let future_subnet = net("10.0.2.0/24");
        let acquired_subnet = pool
            .get(future_subnet)
            .expect("Getting future subnet should succeed");
        assert_eq!(acquired_subnet, future_subnet);

        assert_eq!(pool.get_next(), Some(net("10.0.0.0/24")));
        assert_eq!(pool.get_next(), Some(net("10.0.1.0/24")));
        assert_eq!(pool.get_next(), Some(net("10.0.3.0/24")));
    }

    #[test]
    fn test_get_specific_already_taken_fails() {
        let mut pool = FifoExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let subnet = net("10.0.5.0/24");
        pool.get(subnet).expect("First attempt should succeed");

        match pool.get(subnet) {
            Ok(s) => panic!("Expected an error, but got Ok({:?})", s),
            Err(SubnetPoolError::SubnetNotFree(e)) => assert_eq!(e, subnet),
            Err(e) => panic!("Expected a SubnetNotFree error, but got {:?}", e),
        }
    }

    #[test]
    fn test_get_specific_from_free_list() {
        let mut pool = FifoExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let subnet_a = net("10.0.0.0/24");
        pool.get_next();
        pool.give_back(subnet_a);
        let acquired_subnet = pool
            .get(subnet_a)
            .expect("Getting from free list should succeed");
        assert_eq!(acquired_subnet, subnet_a);
        assert_ne!(pool.get_next(), Some(subnet_a));
    }

    #[test]
    fn test_get_specific_unavailable_fails() {
        let mut pool = FifoExactIpv4SubnetPool::<24>::new(net("10.0.0.0/23")).unwrap();
        let subnet_b = net("10.0.1.0/24");
        pool.get_next();
        pool.get_next();

        match pool.get(subnet_b) {
            Ok(s) => panic!("Expected an error, but got Ok({:?})", s),
            Err(SubnetPoolError::SubnetNotFree(e)) => assert_eq!(e, subnet_b),
            Err(e) => panic!("Expected a SubnetNotFree error, but got {:?}", e),
        }
    }
}
