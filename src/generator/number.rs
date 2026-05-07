use core::hash::Hash as _;
use core::hash::Hasher as _;

use rand::Rng;
use rapidhash::fast::RapidHasher;

#[derive(Debug)]
pub enum Number {
    Constant(u64),
    Uniform(rand::distr::Uniform<u64>),
    Zipfian(Zipfian0),
    /// https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ScrambledZipfianGenerator.java
    ZipfianScrambled(ZipfianScrambled),
    ZipfianLatest(Zipfian1),
}

impl Number {
    #[inline]
    pub fn constant(value: u64) -> Self {
        Self::Constant(value)
    }

    #[inline]
    pub fn uniform(n: u64) -> Self {
        Self::Uniform(rand::distr::Uniform::new(0, n).expect("Invalid uniform upper bound"))
    }

    // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java#L132-L148
    // https://en.wikipedia.org/wiki/Zipf%27s_law
    pub fn zipfian(n: u64, s: f32) -> Self {
        Self::Zipfian(Zipfian0::new(n, s))
    }

    // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java#L132-L148
    // https://en.wikipedia.org/wiki/Zipf%27s_law
    pub fn zipfian_scrambled(n: u64, s: f32) -> Self {
        Self::ZipfianScrambled(ZipfianScrambled {
            n,
            zipfian: Zipfian0::new(n, s),
        })
    }

    pub fn zipfian_latest(n: u64, s: f32) -> Self {
        Self::ZipfianLatest(Zipfian1(Zipfian0::new(n, s)))
    }
}

impl rand::distr::Distribution<u64> for Number {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        match self {
            Number::Constant(value) => *value,
            Number::Uniform(uniform) => uniform.sample(rng),
            Number::Zipfian(zipfian) => zipfian.sample(rng),
            Number::ZipfianScrambled(scrambled) => scrambled.sample(rng),
            Number::ZipfianLatest(latest) => latest.sample(rng),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ZipfianScrambled {
    n: u64,
    zipfian: Zipfian0,
}

impl rand::distr::Distribution<u64> for ZipfianScrambled {
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
