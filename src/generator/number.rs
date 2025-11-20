use rand::distr::Distribution as _;

use crate::generator::Generator;

#[derive(Debug)]
pub enum Number {
    Constant(u64),
    Uniform(rand::distr::Uniform<u64>),
    Zipfian(rand_distr::Zipf<f32>),
}

impl Number {
    #[inline]
    pub fn constant(value: u64) -> Self {
        Self::Constant(value)
    }

    #[inline]
    pub fn uniform(count: u64) -> Self {
        Self::Uniform(rand::distr::Uniform::new(0, count).unwrap())
    }

    // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java#L132-L148
    // https://en.wikipedia.org/wiki/Zipf%27s_law
    pub fn zipfian(n: u64, s: f32) -> Self {
        Self::Zipfian(rand_distr::Zipf::new(n as f32, s).expect("Invalid zipf parameters"))
    }
}

impl Generator for Number {
    type Item = u64;

    #[inline]
    fn next<R: rand::Rng>(&mut self, rng: &mut R) -> Self::Item {
        match self {
            Number::Constant(value) => *value,
            Number::Uniform(uniform) => uniform.sample(rng),
            // https://github.com/brianfrankcooper/YCSB/blob/9858c4dab6dc45991871c9f137bd011752d9c21b/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java#L250-L263
            // Map from range 1..=n to 0..n
            Number::Zipfian(zipfian) => zipfian.sample(rng).floor() as u64 - 1,
        }
    }
}
