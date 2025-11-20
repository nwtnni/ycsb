use rand::Rng;
use rand::distr::weighted::WeightedIndex;

pub(crate) struct Discrete<T> {
    weights: WeightedIndex<f32>,
    values: Vec<T>,
}

impl<T> Discrete<T> {
    #[inline]
    pub fn new(choices: Vec<(T, f32)>) -> Self {
        let weights = WeightedIndex::new(choices.iter().map(|(_, weight)| *weight)).unwrap();
        let values = choices.into_iter().map(|(value, _)| value).collect();
        Self { weights, values }
    }
}

impl<T> rand::distr::Distribution<T> for Discrete<T>
where
    T: Copy,
{
    #[inline]
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> T {
        let index = self.weights.sample(rng);
        self.values[index]
    }
}
