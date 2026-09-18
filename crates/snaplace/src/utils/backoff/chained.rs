use std::{
    fmt::Debug,
    iter::{empty, Chain, Empty, Fuse},
    time::Duration,
};

use backon::{Backoff, BackoffBuilder};
use either::Either;

#[derive(Debug)]
pub struct ChainedBackoff<B1, B2>
where
    B1: Backoff + Debug,
    B2: Backoff + Debug,
{
    iter: Chain<Fuse<B1>, Either<B2, Empty<Duration>>>,
}

impl<B1, B2> Iterator for ChainedBackoff<B1, B2>
where
    B1: Backoff + Debug,
    B2: Backoff + Debug,
{
    type Item = Duration;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[derive(Debug, Clone)]
pub struct ChainedBuilder<B1, B2 = B1>
where
    B1: BackoffBuilder,
    B1::Backoff: Debug,
    B2: BackoffBuilder,
    B2::Backoff: Debug,
{
    head: B1,
    tail: Option<B2>,
}

impl<B1, B2> BackoffBuilder for ChainedBuilder<B1, B2>
where
    B1: BackoffBuilder,
    B1::Backoff: Debug,
    B2: BackoffBuilder,
    B2::Backoff: Debug,
{
    type Backoff = ChainedBackoff<B1::Backoff, B2::Backoff>;

    #[inline]
    fn build(self) -> Self::Backoff {
        ChainedBackoff {
            iter: self.head.build().fuse().chain(
                self.tail
                    .map(|t| Either::Left(t.build()))
                    .unwrap_or_else(|| Either::Right(empty())),
            ),
        }
    }
}

impl<B1, B2> ChainedBuilder<B1, B2>
where
    B1: BackoffBuilder,
    B1::Backoff: Debug,
    B2: BackoffBuilder,
    B2::Backoff: Debug,
{
    pub fn single(b1: B1) -> Self {
        Self {
            head: b1,
            tail: None,
        }
    }

    #[allow(dead_code)]
    pub fn chained(b1: B1, b2: B2) -> Self {
        Self {
            head: b1,
            tail: Some(b2),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use backon::{BackoffBuilder, ConstantBuilder, FibonacciBuilder};
    use tracing::debug;
    use tracing_test::traced_test;

    use super::ChainedBuilder;

    #[traced_test]
    #[test]
    fn display_chained() {
        let delays = ChainedBuilder::chained(
            ConstantBuilder::default()
                .with_delay(Duration::from_millis(150))
                .with_max_times(12),
            ConstantBuilder::default()
                .with_delay(Duration::from_millis(10))
                .with_max_times(38)
                .with_jitter(),
        )
        .build()
        .collect::<Vec<_>>();
        debug!(?delays);

        let delays = ChainedBuilder::<_, ConstantBuilder>::single(
            ConstantBuilder::default()
                .with_delay(Duration::from_millis(5))
                .with_max_times(50)
                .with_jitter(),
        )
        .build()
        .collect::<Vec<_>>();
        debug!(?delays);

        let delays = ChainedBuilder::chained(
            FibonacciBuilder::default()
                .with_min_delay(Duration::from_millis(10))
                .with_max_delay(Duration::from_millis(250))
                .with_max_times(12)
                .with_jitter(),
            ConstantBuilder::default()
                .with_delay(Duration::from_millis(10))
                .with_max_times(38)
                .with_jitter(),
        )
        .build()
        .collect::<Vec<_>>();
        debug!(?delays);
    }
}
