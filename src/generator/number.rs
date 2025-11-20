use core::hash::Hash as _;
use core::hash::Hasher as _;

use rand::Rng;
use rapidhash::RapidHasher;

#[derive(Debug)]
pub(crate) enum Number {
    Constant(u64),
    Uniform(rand::distr::Uniform<u64>),
    Zipfian(Zipfian0),
    /// https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ScrambledZipfianGenerator.java
    ZipfianScrambled(ZipfianScrambled),
    ZipfianLatest(ZipfianLatest),
}

impl Number {
    #[inline]
    #[expect(dead_code)]
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
        rand_distr::Zipf::new(n as f32, s)
            .map(Zipfian1)
            .map(Zipfian0)
            .map(Self::Zipfian)
            .expect("Invalid zipf parameters")
    }

    // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java#L132-L148
    // https://en.wikipedia.org/wiki/Zipf%27s_law
    pub fn zipfian_scrambled(n: u64, s: f32) -> Self {
        rand_distr::Zipf::new(n as f32, s)
            .map(|zipfian| ZipfianScrambled { n, zipfian })
            .map(Self::ZipfianScrambled)
            .expect("Invalid zipf parameters for zipfian scrambled")
    }

    pub fn zipfian_latest(n: u64, s: f32) -> Self {
        rand_distr::Zipf::new(n as f32, s)
            .map(Zipfian1)
            .map(|zipfian| ZipfianLatest { n, zipfian })
            .map(Self::ZipfianLatest)
            .expect("Invalid zipf parameters for latest")
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
pub(crate) struct ZipfianLatest {
    n: u64,
    zipfian: Zipfian1,
}

impl rand::distr::Distribution<u64> for ZipfianLatest {
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        self.n - self.zipfian.sample(rng)
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct ZipfianScrambled {
    n: u64,
    zipfian: rand_distr::Zipf<f32>,
}

impl rand::distr::Distribution<u64> for ZipfianScrambled {
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        let sample = self.zipfian.sample(rng).to_bits();
        let mut hasher = RapidHasher::default();
        sample.hash(&mut hasher);
        hasher.finish() % self.n
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug)]
pub(crate) struct Zipfian0(Zipfian1);

impl rand::distr::Distribution<u64> for Zipfian0 {
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        self.0.sample(rng) - 1
    }
}

#[repr(transparent)]
#[derive(Copy, Clone, Debug)]
pub(crate) struct Zipfian1(rand_distr::Zipf<f32>);

impl rand::distr::Distribution<u64> for Zipfian1 {
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        self.0.sample(rng).floor() as u64
    }
}
