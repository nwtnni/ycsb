use std::io;
use std::io::BufWriter;
use std::io::Write as _;

use pico_args::Arguments;
use rand::distr::Distribution;
use rapidhash::rng::RapidRng;
use ycsb::generator;

struct Cli {
    seed: u64,
    count: usize,
    n: u64,
    s: Vec<f32>,
}

fn main() {
    let mut args = Arguments::from_env();

    let cli = Cli {
        seed: args
            .opt_value_from_str("--seed")
            .expect("Failed to parse --seed <u64>")
            .unwrap_or(0xdeadbeef),
        count: args
            .value_from_str("--count")
            .expect("Failed to parse --count <usize>"),
        n: args
            .value_from_str("-n")
            .expect("Failed to parse -n <usize>"),
        s: args
            .values_from_str("-s")
            .expect("Failed to parse -s <f32>"),
    };

    let mut rng = RapidRng::new(cli.seed);
    let mut stdout = BufWriter::new(io::stdout().lock());

    for s in cli.s {
        let distribution = generator::Number::zipfian(cli.n, s);
        let prefix = format!("{s},");
        for _ in 0..cli.count {
            let sample = distribution.sample(&mut rng);
            writeln!(&mut stdout, "{prefix}{sample}").unwrap();
        }
    }
}
