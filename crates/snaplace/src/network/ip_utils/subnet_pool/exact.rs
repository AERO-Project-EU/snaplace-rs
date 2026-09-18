use std::{
    collections::{BTreeSet, HashSet},
    iter::Peekable,
};

use ipnet::{Ipv4Net, Ipv4Subnets};

use super::SubnetPoolError;

/// IPv4 subnet pool that recycles subnets (by always "consuming"/returning the smallest subnet
/// address first), which also allows requesting a specific subnet.
///
/// # Invariant for correctness
///
/// The [`Ipv4Net`] argument provided to [`ExactIpv4SubnetPool::give_back`] must have been returned
/// (at some point earlier) by [`ExactIpv4SubnetPool::get_next`] or [`ExactIpv4SubnetPool::get`].
/// Otherwise, it will be double-counted as free, hence future calls of the two latter methods
/// might return it (at most) twice.
///
/// On debug builds (i.e., when `debug_assertions` is enabled), [`ExactIpv4SubnetPool::give_back`]
/// panics if the provided `subnet` has not been acquired via [`ExactIpv4SubnetPool::get_next`] or
/// [`ExactIpv4SubnetPool::get`].
#[derive(Debug)]
pub struct ExactIpv4SubnetPool<const P: u8> {
    iter: Peekable<Ipv4Subnets>,
    free: BTreeSet<Ipv4Net>,
    taken: HashSet<Ipv4Net, crate::BuildHasher>,
}

impl<const P: u8> ExactIpv4SubnetPool<P> {
    pub fn new(subnet: Ipv4Net) -> Result<Self, SubnetPoolError> {
        Ok(Self {
            iter: subnet.subnets(P)?.peekable(),
            free: Default::default(),
            taken: Default::default(),
        })
    }

    #[inline]
    pub fn get_next(&mut self) -> Option<Ipv4Net> {
        match self.free.pop_first() {
            Some(subnet) => Some(subnet),
            None => loop {
                match self.iter.next() {
                    Some(subnet) if self.taken.contains(&subnet) => continue,
                    subnet => return subnet,
                }
            },
        }
    }

    /// # Panics
    ///
    /// On debug builds (i.e., when `debug_assertions` is enabled), this method panics if the
    /// provided `subnet` has not been acquired via [`ExactIpv4SubnetPool::get_next`] or
    /// [`ExactIpv4SubnetPool::get`].
    #[inline]
    pub fn give_back(&mut self, subnet: Ipv4Net) {
        if self.taken.remove(&subnet) {
            // TODO(ckatsak): Rust edition 2024 syntax -> if let Some() && ... { ... }
            match self.iter.peek() {
                Some(next_subnet) if subnet >= *next_subnet => return,
                _ => {} // fallthrough
            }
        }
        let _just_inserted = self.free.insert(subnet);
        debug_assert!(_just_inserted);
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
                if self.free.remove(&subnet) {
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

    use super::{ExactIpv4SubnetPool, SubnetPoolError};

    /// Helper to create an Ipv4Net for tests, panicking on failure.
    fn net(addr: &str) -> Ipv4Net {
        Ipv4Net::from_str(addr).expect("Failed to parse network string")
    }

    #[test]
    fn test_new_pool() {
        assert!(ExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).is_ok());
        assert!(ExactIpv4SubnetPool::<22>::new(net("10.0.0.0/24")).is_err());
    }

    #[test]
    fn test_get_next_simple_sequence_and_exhaustion() {
        let mut pool = ExactIpv4SubnetPool::<30>::new(net("192.168.0.0/29")).unwrap();
        assert_eq!(pool.get_next(), Some(net("192.168.0.0/30")));
        assert_eq!(pool.get_next(), Some(net("192.168.0.4/30")));
        assert_eq!(pool.get_next(), None);
    }

    #[test]
    fn test_get_next_from_free_list_is_sorted() {
        let mut pool = ExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let subnet_a = pool.get_next().unwrap(); // 10.0.0.0/24
        let subnet_b = pool.get_next().unwrap(); // 10.0.1.0/24

        pool.give_back(subnet_b);
        pool.give_back(subnet_a);

        assert_eq!(pool.get_next(), Some(subnet_a));
        assert_eq!(pool.get_next(), Some(subnet_b));
    }

    #[test]
    fn test_get_specific_at_iterator_head() {
        let mut pool = ExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let expected_subnet = net("10.0.0.0/24");

        // Request the exact subnet that is next in the iterator
        let acquired_subnet = pool
            .get(expected_subnet)
            .expect("Getting the head of the iterator should succeed");
        assert_eq!(acquired_subnet, expected_subnet);

        // The next automatic one should now be the following subnet
        assert_eq!(pool.get_next(), Some(net("10.0.1.0/24")));
    }

    #[test]
    fn test_get_specific_ahead_of_iterator_and_skip() {
        let mut pool = ExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let future_subnet = net("10.0.2.0/24");

        // Get a subnet far ahead in the sequence and assert its value
        let acquired_subnet = pool
            .get(future_subnet)
            .expect("Getting a future subnet should succeed");
        assert_eq!(acquired_subnet, future_subnet);

        // get_next should now provide the subnets *around* the taken one
        assert_eq!(pool.get_next(), Some(net("10.0.0.0/24")));
        assert_eq!(pool.get_next(), Some(net("10.0.1.0/24")));
        assert_eq!(pool.get_next(), Some(net("10.0.3.0/24"))); // Skips .2
    }

    #[test]
    fn test_get_specific_already_taken_fails() {
        let mut pool = ExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let subnet = net("10.0.5.0/24");

        // Take it once, asserting success by unwrapping
        pool.get(subnet)
            .expect("First attempt to get subnet should succeed");

        // Try to take it again, asserting the specific error type via a match
        match pool.get(subnet) {
            Ok(s) => panic!("Expected an error, but got Ok({:?})", s),
            Err(SubnetPoolError::SubnetNotFree(e)) => {
                assert_eq!(e, subnet, "Error should contain the correct subnet")
            }
            Err(e) => panic!("Expected a SubnetNotFree error, but got {:?}", e),
        }
    }

    #[test]
    fn test_get_specific_from_free_list() {
        let mut pool = ExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let subnet_a = net("10.0.0.0/24");

        assert_eq!(pool.get_next(), Some(subnet_a));
        pool.give_back(subnet_a);

        // Specifically request it; assert success and value by unwrapping
        let acquired_subnet = pool
            .get(subnet_a)
            .expect("Getting a subnet from the free list should succeed");
        assert_eq!(acquired_subnet, subnet_a);

        assert_ne!(pool.get_next(), Some(subnet_a));
    }

    #[test]
    fn test_get_specific_unavailable_fails() {
        let mut pool = ExactIpv4SubnetPool::<24>::new(net("10.0.0.0/23")).unwrap();
        let subnet_b = net("10.0.1.0/24");
        let unavailable_subnet = net("10.0.2.0/24"); // Outside the /23 parent

        // Exhaust the pool
        pool.get_next(); // 10.0.0.0/24
        pool.get_next(); // 10.0.1.0/24

        // Try to get subnet B, which is taken but not in free list
        match pool.get(subnet_b) {
            Ok(s) => panic!(
                "Expected an error for unavailable subnet, but got Ok({:?})",
                s
            ),
            Err(SubnetPoolError::SubnetNotFree(e)) => assert_eq!(e, subnet_b),
            Err(e) => panic!("Expected a SubnetNotFree error, but got {:?}", e),
        }

        // Try to get a subnet that never existed in the pool
        match pool.get(unavailable_subnet) {
            Ok(s) => panic!(
                "Expected an error for out-of-bounds subnet, but got Ok({:?})",
                s
            ),
            Err(SubnetPoolError::SubnetNotFree(e)) => assert_eq!(e, unavailable_subnet),
            Err(e) => panic!("Expected a SubnetNotFree error, but got {:?}", e),
        }
    }

    #[test]
    fn test_give_back_ahead_of_iterator() {
        let mut pool = ExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let future_subnet = net("10.0.5.0/24");

        pool.get(future_subnet).unwrap();
        assert!(pool.taken.contains(&future_subnet));

        pool.give_back(future_subnet);
        assert!(!pool.taken.contains(&future_subnet));
        assert!(pool.free.is_empty());

        assert_eq!(pool.get_next(), Some(net("10.0.0.0/24")));
    }

    #[test]
    fn test_give_back_behind_iterator() {
        let mut pool = ExactIpv4SubnetPool::<24>::new(net("10.0.0.0/16")).unwrap();
        let subnet_a = pool.get_next().unwrap();
        pool.get_next().unwrap();

        pool.give_back(subnet_a);
        assert!(pool.free.contains(&subnet_a));

        assert_eq!(pool.get_next(), Some(subnet_a));
    }
}
