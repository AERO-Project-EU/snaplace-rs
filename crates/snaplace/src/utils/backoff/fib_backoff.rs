use std::{iter::FusedIterator, time::Duration};

#[derive(Debug, Clone, Copy)]
pub struct FibonacciBackoff {
    a: Duration,
    b: Duration,
}

impl FibonacciBackoff {
    #[inline]
    pub fn new(b: Duration) -> Self {
        Self {
            a: Duration::ZERO,
            b,
        }
    }
}

impl Iterator for FibonacciBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.a + self.b;
        self.a = self.b;
        self.b = ret;
        Some(ret)
    }
}

impl FusedIterator for FibonacciBackoff {}
