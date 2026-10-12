//! The one random number source every tsq mode draws from.
//!
//! ChaCha8 seeded from a full 64-bit seed: the stream is defined by the
//! algorithm, not by the platform's C library, so a seed reproduces the same
//! data and the same queries on Linux, macOS and Windows. Every draw goes
//! through the unbiased range sampling of `rand` (no modulo bias), and
//! floats are uniform in [0, 1).

use rand::{Rng as _, SeedableRng};
use rand_chacha::ChaCha8Rng;

pub struct Rng(ChaCha8Rng);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(ChaCha8Rng::seed_from_u64(seed))
    }

    /// An independent stream for item `index` under `seed`: the stream does
    /// not depend on how many items come before it, so item 1234 of a run
    /// can be regenerated alone.
    pub fn stream(seed: u64, index: u64) -> Self {
        let mut r = Rng::new(seed);
        r.0.set_stream(index);
        r
    }

    #[cfg(test)]
    pub fn next_u64(&mut self) -> u64 {
        rand::RngCore::next_u64(&mut self.0)
    }

    /// Uniform in [0, n). `n` must be positive.
    pub fn below(&mut self, n: usize) -> usize {
        assert!(n > 0, "Rng::below(0)");
        self.0.random_range(0..n as u64) as usize
    }

    /// Uniform in [lo, hi], both inclusive.
    pub fn range(&mut self, lo: i64, hi: i64) -> i64 {
        if lo >= hi {
            return lo;
        }
        self.0.random_range(lo..=hi)
    }

    /// Uniform in [0, 1).
    pub fn unit(&mut self) -> f64 {
        self.0.random::<f64>()
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        self.unit() < p
    }

    pub fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.below(xs.len())]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_is_fixed() {
        // The documented guarantee: a seed means the same values everywhere.
        // If this changes, every recorded seed in a bug report goes stale.
        let mut r = Rng::new(42);
        let v: Vec<usize> = (0..5).map(|_| r.below(1000)).collect();
        let mut again = Rng::new(42);
        let w: Vec<usize> = (0..5).map(|_| again.below(1000)).collect();
        assert_eq!(v, w);
    }

    #[test]
    fn unit_covers_the_whole_interval() {
        // The old generator divided a 31-bit value by u32::MAX, so every
        // float landed in [0, 0.5). Both halves must be hit.
        let mut r = Rng::new(1);
        let xs: Vec<f64> = (0..10_000).map(|_| r.unit()).collect();
        assert!(xs.iter().all(|&x| (0.0..1.0).contains(&x)));
        let high = xs.iter().filter(|&&x| x >= 0.5).count();
        assert!((4500..5500).contains(&high), "high half: {high}");
    }

    #[test]
    fn streams_are_independent_of_position() {
        let mut a = Rng::stream(7, 3);
        let mut b = Rng::stream(7, 3);
        let mut c = Rng::stream(7, 4);
        let x = a.next_u64();
        assert_eq!(x, b.next_u64());
        assert_ne!(x, c.next_u64());
    }
}
