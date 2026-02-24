#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pandas",
#     "matplotlib",
#     "numpy",
# ]
# ///
"""
Quantile plot for selected frequency slices of the CMS point query profile.

For each requested true frequency, finds the nearest item and plots
its error quantile function from min to max. Multiple slices are
overlaid for comparison.

Usage:
    uv run plot_cms_frequency_slice.py <input.tsv> <freq1,freq2,...> [output.svg]

Example:
    uv run plot_cms_frequency_slice.py cpp/results/cms_point_query.tsv 10,50,100,500
"""

import sys
import logging

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def parse_metadata(filepath):
    """Parse # key=value comment lines from the TSV file."""
    meta = {}
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line.startswith("#"):
                break
            if "=" in line:
                key, val = line.lstrip("# ").split("=", 1)
                meta[key.strip()] = val.strip()
    return meta


def main():
    if len(sys.argv) < 3:
        print("Usage: plot_cms_frequency_slice.py <input.tsv> <freq1,freq2,...> [output.svg]")
        sys.exit(1)

    input_file = sys.argv[1]
    target_freqs = [float(f) for f in sys.argv[2].split(",")]
    output_file = sys.argv[3] if len(sys.argv) > 3 else "cms_quantile_slices.svg"

    logging.info(f"Loading data from {input_file}...")
    meta = parse_metadata(input_file)
    df = pd.read_csv(input_file, sep="\t", comment="#")
    logging.info(f"Loaded {len(df)} items")

    error_bound = float(meta.get("error_bound", "nan"))
    zipf_exp = meta.get("zipf_exponent", "?")

    # All quantile columns, sorted by level
    q_cols = sorted(
        [c for c in df.columns if c.startswith("Q_")],
        key=lambda c: float(c.split("_", 1)[1]),
    )
    q_levels = np.array([float(c.split("_", 1)[1]) for c in q_cols])

    logging.info(f"Found {len(q_cols)} quantile levels: "
                 f"{q_levels[0]:.4f} to {q_levels[-1]:.4f}")

    # Plot setup
    plt.style.use("seaborn-v0_8-paper")
    plt.rcParams.update({
        "font.size": 11,
        "axes.labelsize": 12,
        "axes.titlesize": 13,
        "legend.fontsize": 9,
    })

    cmap = plt.cm.viridis
    colors = [cmap(i / max(len(target_freqs) - 1, 1))
              for i in range(len(target_freqs))]

    fig, ax = plt.subplots(figsize=(10, 7), dpi=100)

    for target_freq, color in zip(target_freqs, colors):
        idx = (df["TrueFreq"] - target_freq).abs().idxmin()
        row = df.iloc[idx]
        true_freq = row["TrueFreq"]

        # Error values at each quantile level
        error_values = np.array([row[c] for c in q_cols])

        ax.plot(q_levels, error_values, "o-", color=color, linewidth=1.8,
                markersize=4, label=f"$c_j$ = {true_freq:.0f}")

    # Theoretical error bound
    if not np.isnan(error_bound):
        ax.axhline(error_bound, color="#2166ac", linewidth=2.0,
                   linestyle="-.", alpha=0.8,
                   label=r"$\varepsilon N$" + f" = {error_bound:.0f}")

    # Zero error reference
    ax.axhline(0, color="black", linewidth=0.8, linestyle="-", alpha=0.3)

    # X-axis: percentile labels
    ax.set_xlabel("Quantile level (percentile)", fontweight="semibold")
    ax.set_ylabel("Absolute error (estimate $-$ true count)", fontweight="semibold")
    ax.set_title(
        f"CMS Error Quantile Profile at Selected Frequencies (Zipf = {zipf_exp})",
        fontweight="bold", pad=15,
    )

    # Format x-axis as percentages
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))

    ax.legend(loc="upper left", frameon=True, fancybox=False, framealpha=0.95)
    ax.grid(True, which="major", alpha=0.3, linewidth=0.8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()

    logging.info(f"Saving figure to {output_file}...")
    fig.savefig(output_file, format="svg", bbox_inches="tight")
    logging.info("Done!")


if __name__ == "__main__":
    main()
