import sys
import plotly.express as px
import polars as pl


COUNT = 10


def main():
    df = pl.scan_csv(sys.argv[1], has_header=False).rename(
        dict(column_1="n", column_2="s")
    )

    samples = len(df.collect_schema().names()) - 2

    # Convert from wide to long format
    df = df.unpivot(index=["n", "s"]).drop("variable")

    df = (
        # Compute probability density
        df.group_by("n", "s", "value")
        .len()
        .with_columns(probability=pl.col("len") / samples)
        .sort("s", "value")
        .group_by("n", "s", maintain_order=True)
        .agg(
            pl.struct(pl.all())
            # Compute cumulative probability (must sort by value)
            # https://github.com/pola-rs/polars/issues/12262
            .struct.with_fields(cumulative=pl.field("probability").cum_sum())
            .struct.unnest()
            .bottom_k_by("value", COUNT)
        )
        .explode("value", "len", "probability", "cumulative")
        .cast(dict(value=pl.String))
    )

    df = df.collect()

    for (n,), group in df.group_by("n", maintain_order=True):
        plot(n, samples, group, "probability", "Probability mass")
        plot(n, samples, group, "cumulative", "Cumulative distribution")


def split_axes(fig):
    fig.update_xaxes(matches=None)
    fig.update_yaxes(matches=None)
    fig.for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))


def plot(n, samples, df, y, title):
    fig = px.bar(
        df,
        x="value",
        y=y,
        facet_col="s",
        facet_col_spacing=0.06,
        title=f"{title} function of YCSB Zipf implementation (N={n:.0e}, lowest {COUNT} keys, {samples:.0e} samples)",
    )
    split_axes(fig)
    fig.update_yaxes(title=title, row=1, col=1)
    fig.update_layout(width=1080)
    fig.write_image(f"{y}-{n:.0e}.png")


if __name__ == "__main__":
    main()
