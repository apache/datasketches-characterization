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
Plot CMS point query profile from Experiment 1 output.

Reads a TSV file produced by the cms-point-query profile and generates
an SVG showing per-item estimated frequency quantile bands (-3s to +3s).

The x-axis shows items ordered by ascending true frequency.
The y-axis shows estimated frequency (error quantile + true freq).

Usage:
    uv run plot_cms_point_query.py [input.tsv] [output.svg]
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
    input_file = sys.argv[1] if len(sys.argv) > 1 else "cms_point_query.tsv"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "cms_point_query_error.svg"

    logging.info(f"Loading data from {input_file}...")
    meta = parse_metadata(input_file)
    df = pd.read_csv(input_file, sep="\t", comment="#")
    logging.info(f"Loaded {len(df)} items")

    # Extract bound violation stats from metadata
    theoretical_delta = float(meta.get("theoretical_delta", "nan"))
    actual_violation_frac = float(meta.get("actual_violation_frac", "nan"))
    error_bound = float(meta.get("error_bound", "nan"))

    logging.info(
        f"Theoretical delta={theoretical_delta}, "
        f"actual violation frac={actual_violation_frac:.6f}, "
        f"error bound={error_bound:.2f}"
    )

    # All quantile columns from TSV
    all_q_cols = [c for c in df.columns if c.startswith("Q_")]
    all_q_levels = {float(c.split("_", 1)[1]): c for c in all_q_cols}

    # Select 9 quantile levels: min, 3 sigma pairs, median, max
    sigma_targets = [0.0, 0.00135, 0.02275, 0.15866, 0.5, 0.84134, 0.97725, 0.99865, 1.0]
    q_cols = []
    for target in sigma_targets:
        closest = min(all_q_levels.keys(), key=lambda q: abs(q - target))
        q_cols.append(all_q_levels[closest])

    # Convert error quantiles to estimated frequency: est = error + true_freq
    true_freq = df["TrueFreq"].values
    est_cols = []
    for col in q_cols:
        est_name = col.replace("Q_", "Est_")
        df[est_name] = df[col] + true_freq
        est_cols.append(est_name)

    # x-axis: true frequency (ascending, already sorted)
    x = true_freq

    # Plot setup
    plt.style.use("seaborn-v0_8-paper")
    plt.rcParams.update(
        {
            "font.size": 11,
            "axes.labelsize": 12,
            "axes.titlesize": 13,
            "legend.fontsize": 9,
            "xtick.labelsize": 10,
            "ytick.labelsize": 10,
        }
    )

    fig, ax = plt.subplots(figsize=(12, 7), dpi=100)

    # Use log-log scale
    ax.set_xscale("log")
    ax.set_yscale("log")

    # Filter to positive true frequencies for log scale
    mask = x > 0
    x_pos = x[mask]
    df_pos = df[mask].copy()

    bound_color = "#2166ac"

    # Lower bound: true frequency (y = x, no error)
    ax.plot(x_pos, x_pos, color=bound_color, linewidth=2.0, linestyle="--",
            alpha=0.9, label=r"Lower bound: $c_j$", zorder=10)

    # Upper bound: true frequency + epsilon*N
    if not np.isnan(error_bound):
        ax.plot(x_pos, x_pos + error_bound, color=bound_color, linewidth=2.0,
                linestyle="-.", alpha=0.9,
                label=r"Upper bound: $c_j + \varepsilon N$", zorder=10)

    # Shading between symmetric quantile pairs (outer to inner)
    # Indices: 0=min, 1=-3σ, 2=-2σ, 3=-1σ, 4=median, 5=+1σ, 6=+2σ, 7=+3σ, 8=max
    band_specs = [
        (est_cols[0], est_cols[8], "#efedf5", 0.4, "Min/Max"),
        (est_cols[1], est_cols[7], "#dadaeb", 0.5, "Q0.1% / Q99.9%"),
        (est_cols[2], est_cols[6], "#bcbddc", 0.6, "Q2.3% / Q97.7%"),
        (est_cols[3], est_cols[5], "#9e9ac8", 0.7, "Q15.9% / Q84.1%"),
    ]
    for lo, hi, color, alpha, label in band_specs:
        ax.fill_between(x_pos, df_pos[lo], df_pos[hi],
                        color=color, alpha=alpha, label=label)

    # Quantile lines
    line_styles = [":", ":", "--", "-.", "-", "-.", "--", ":", ":"]
    line_alphas = [0.4, 0.5, 0.6, 0.8, 1.0, 0.8, 0.6, 0.5, 0.4]
    line_widths = [0.6, 0.8, 0.8, 1.0, 2.0, 1.0, 0.8, 0.8, 0.6]
    line_color = "#54278f"

    for i, (col, ls, a, lw) in enumerate(
        zip(est_cols, line_styles, line_alphas, line_widths)
    ):
        if i == 4:  # median gets special treatment
            ax.plot(
                x_pos, df_pos[col], color="#e31a1c", linewidth=lw,
                linestyle=ls, alpha=a, label="Median estimate",
            )
        else:
            ax.plot(
                x_pos, df_pos[col], color=line_color, linewidth=lw,
                linestyle=ls, alpha=a,
            )

    # Bound violation annotation in legend
    if not np.isnan(theoretical_delta) and not np.isnan(actual_violation_frac):
        ax.plot([], [], ' ',
                label=(f"Bound violations: "
                       f"theoretical={theoretical_delta:.2%}, "
                       f"actual={actual_violation_frac:.4%}"))

    ax.set_xlabel("True frequency (log scale)", fontweight="semibold")
    ax.set_ylabel("Estimated frequency (log scale)", fontweight="semibold")
    zipf_exp = meta.get("zipf_exponent", "?")
    ax.set_title(
        f"CMS Point Query Estimated Frequency Profile (Zipf exponent = {zipf_exp})",
        fontweight="bold", pad=15,
    )

    ax.legend(loc="upper left", frameon=True, fancybox=False, framealpha=0.95)
    ax.grid(True, which="major", alpha=0.3, linewidth=0.8)
    ax.grid(True, which="minor", alpha=0.15, linewidth=0.5, linestyle=":")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()

    logging.info(f"Saving figure to {output_file}...")
    fig.savefig(output_file, format="svg", bbox_inches="tight")
    logging.info("Done!")


if __name__ == "__main__":
    main()
