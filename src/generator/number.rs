use core::hash::Hash as _;
use core::hash::Hasher as _;

use rand::Rng;
use rapidhash::fast::RapidHasher;

#[derive(Debug)]
pub enum Number {
    Constant(u64),
    Uniform(rand::distr::Uniform<u64>),
    Zipfian0(Zipfian0),
    /// https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ScrambledZipfianGenerator.java
    Zipfian0Scrambled(Zipfian0Scrambled),
    Zipfian1(Zipfian1),
}

impl Number {
    pub fn constant(value: u64) -> Self {
        Self::Constant(value)
    }

    pub fn uniform(n: u64) -> Self {
        Self::Uniform(rand::distr::Uniform::new(0, n).expect("Invalid uniform upper bound"))
    }

    /// [Zipfian](https://en.wikipedia.org/wiki/Zipf%27s_law) distribution
    /// over `0..n` with exponent `s`. **Panics if s == 1.**
    ///
    /// https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java#L132-L148
    pub fn zipfian_0(n: u64, s: f32) -> Self {
        Self::Zipfian0(Zipfian0::new(n, s))
    }

    /// [Zipfian](https://en.wikipedia.org/wiki/Zipf%27s_law) distribution
    /// over `0..n` with exponent `s`, but hashes to distribute skew across
    /// key range. **Panics if s == 1.**
    ///
    /// https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java#L132-L148
    pub fn zipfian_0_scrambled(n: u64, s: f32) -> Self {
        Self::Zipfian0Scrambled(Zipfian0Scrambled {
            n,
            zipfian: Zipfian0::new(n, s),
        })
    }

    /// [Zipfian](https://en.wikipedia.org/wiki/Zipf%27s_law) distribution
    /// over `1..=n` with exponent `s`. **Panics if s == 1.**
    pub fn zipfian_1(n: u64, s: f32) -> Self {
        Self::Zipfian1(Zipfian1(Zipfian0::new(n, s)))
    }
}

impl rand::distr::Distribution<u64> for Number {
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        match self {
            Number::Constant(value) => *value,
            Number::Uniform(uniform) => uniform.sample(rng),
            Number::Zipfian0(zipfian) => zipfian.sample(rng),
            Number::Zipfian0Scrambled(zipfian) => zipfian.sample(rng),
            Number::Zipfian1(zipfian) => zipfian.sample(rng),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Zipfian0Scrambled {
    n: u64,
    zipfian: Zipfian0,
}

impl rand::distr::Distribution<u64> for Zipfian0Scrambled {
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        let sample = self.zipfian.sample(rng);
        let mut hasher = RapidHasher::default();
        sample.hash(&mut hasher);
        hasher.finish() % self.n
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug)]
pub struct Zipfian1(Zipfian0);

impl rand::distr::Distribution<u64> for Zipfian1 {
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        self.0.sample(rng) + 1
    }
}

#[derive(Copy, Clone, Debug)]
pub struct Zipfian0 {
    n: f32,
    cutoff_1: f32,
    alpha: f32,
    eta: f32,
    zeta: f32,
}

impl Zipfian0 {
    // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java#L132-L148
    fn new(n: u64, s: f32) -> Self {
        assert!(s != 1.0, "Zipfian implementation does not handle s == 1");

        let theta = s;
        let alpha = 1.0 / (1.0 - theta);
        let zeta_n = Self::zeta(n, theta);
        let zeta_2 = Self::zeta(2, theta);
        let n = n as f32;
        let eta = (1.0 - (2.0 / n).powf(1.0 - theta)) / (1.0 - zeta_2 / zeta_n);
        Self {
            n,
            cutoff_1: 1.0 + 0.5f32.powf(theta),
            alpha,
            eta,
            zeta: zeta_n,
        }
    }

    // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java#L198-L208
    fn zeta(n: u64, theta: f32) -> f32 {
        (1..=n).map(|i| i as f32).map(|i| 1.0 / i.powf(theta)).sum()
    }
}

impl rand::distr::Distribution<u64> for Zipfian0 {
    // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java#L250-L263
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        let u = rng.random::<f32>();
        let uz = u * self.zeta;

        if uz < 1.0 {
            return 0;
        }

        if uz < self.cutoff_1 {
            return 1;
        }

        (self.n * (self.eta * (u - 1.0) + 1.0).powf(self.alpha)) as u64
    }
}
