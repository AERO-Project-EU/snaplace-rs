use std::marker::PhantomData;

#[cfg(debug_assertions)]
use average::Max;
use average::{Estimate, Min, Quantile, Variance};
use num_traits::ToPrimitive;

// - debug:   size = 680, align = 0x8
// - release: size = 352, align = 0x8
#[derive(Debug, Clone)]
pub(crate) struct RollingStats<V> {
    var: Variance,
    min: Min,
    p50: Quantile,
    #[cfg(debug_assertions)]
    p90: Quantile,
    #[cfg(debug_assertions)]
    p95: Quantile,
    p99: Quantile,
    #[cfg(debug_assertions)]
    max: Max,
    _typ: PhantomData<V>,
}

impl<V> Default for RollingStats<V> {
    fn default() -> Self {
        Self {
            var: Variance::new(),
            min: Min::new(),
            p50: Quantile::new(0.5),
            #[cfg(debug_assertions)]
            p90: Quantile::new(0.9),
            #[cfg(debug_assertions)]
            p95: Quantile::new(0.95),
            p99: Quantile::new(0.99),
            #[cfg(debug_assertions)]
            max: Max::new(),
            _typ: PhantomData,
        }
    }
}

impl<V: ToPrimitive> RollingStats<V> {
    pub fn add(&mut self, v: V) {
        // SAFETY NOTE: For now, that we are only using unsigned integers for V, conversions to
        // `f64` should be infallible casts:
        //   <https://docs.rs/num-traits/0.2.19/src/num_traits/cast.rs.html#229-267>
        // But (**if** it even makes sense to keep `V` a generic throughout `crate::metrics`) we
        // should probably deal with fallible conversions properly.
        let v = v.to_f64().expect("should convert to f64 flawlessly");
        self.var.add(v);
        self.min.add(v);
        self.p50.add(v);
        #[cfg(debug_assertions)]
        self.p90.add(v);
        #[cfg(debug_assertions)]
        self.p95.add(v);
        self.p99.add(v);
        #[cfg(debug_assertions)]
        self.max.add(v);
    }
}

impl<V> RollingStats<V> {
    #[inline]
    pub fn new() -> Self {
        Default::default()
    }

    #[inline]
    pub fn sample_size(&self) -> u64 {
        self.var.len()
    }
    #[inline]
    pub fn mean(&self) -> f64 {
        self.var.mean()
    }
    #[inline]
    pub fn var(&self) -> f64 {
        self.var.sample_variance()
    }
    #[inline]
    pub fn std(&self) -> f64 {
        self.var.sample_variance().sqrt()
    }
    #[inline]
    pub fn min(&self) -> f64 {
        self.min.min()
    }
    #[inline]
    pub fn p50(&self) -> f64 {
        self.p50.quantile()
    }
    #[cfg(debug_assertions)]
    #[inline]
    pub fn p90(&self) -> f64 {
        self.p90.quantile()
    }
    #[cfg(debug_assertions)]
    #[inline]
    pub fn p95(&self) -> f64 {
        self.p95.quantile()
    }
    #[inline]
    pub fn p99(&self) -> f64 {
        self.p99.quantile()
    }
    #[cfg(debug_assertions)]
    #[inline]
    pub fn max(&self) -> f64 {
        self.max.max()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use rand::{rngs::SmallRng, RngExt};
    use tracing::{debug, info, trace};
    use tracing_test::traced_test;

    use crate::metrics::RollingStats;

    #[traced_test]
    #[test]
    fn bench_rolling_lat() {
        const NUM_OBSERV: u32 = 8 * 1024;

        let mut m = RollingStats::default();
        let mut rng: SmallRng = rand::make_rng();
        let xs = (0..NUM_OBSERV)
            .map(|_| rng.random_range(10000000..3000000000u128))
            .collect::<Vec<_>>();

        let ts_start = Instant::now();
        xs.into_iter().for_each(|x| m.add(x));
        let elapsed = ts_start.elapsed();

        let elapsed_per_observ = elapsed / NUM_OBSERV;
        info!("Elapsed per observation: {elapsed_per_observ:?}");
        // On icy:
        //  - debug:  ~830 ns/obs
        //  - release: ~52 ns/obs

        let _max = 0.;
        #[cfg(debug_assertions)]
        let _max = m.max();
        debug!(
            "Rolling stats:\n - size: {}\n - mean: {}\n - std: {}\n - median: {}\n - min: {}\n - p99: {}\n - max: {_max}",
            m.sample_size(),
            m.mean(),
            m.std(),
            m.p50(),
            m.min(),
            m.p99(),
        );
        trace!("{m:?}");
    }
}
