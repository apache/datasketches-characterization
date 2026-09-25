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
Plot CMS point query profiles across multiple skew regimes.

Reads TSV files (one per skew regime) produced by the cms-point-query profile
and generates two SVGs:
  1. cms_point_query_error.svg — estimated frequency vs true frequency (sigma bands)
  2. cms_rel_error_vs_freq_rank.svg — relative error vs frequency rank

Usage:
    uv run plot_cms_point_query.py high_skew.tsv medium_skew.tsv low_skew.tsv
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


def load_regime(filepath):
    """Load a single TSV regime file, return (df, metadata)."""
    meta = parse_metadata(filepath)
    df = pd.read_csv(filepath, sep="\t", comment="#")
    return df, meta


def plot_estimated_frequency(regimes, output_file):
    """
    Plot estimated frequency (true_freq + abs_error) vs true frequency.
    Matches the PR 104 style: log-log, sigma bands, y=x and y=x+εN bounds.
    """
    plt.style.use("seaborn-v0_8-paper")
    plt.rcParams.update({
        "font.size": 10,
        "axes.labelsize": 11,
        "axes.titlesize": 12,
        "legend.fontsize": 8,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
    })

    fig, axes = plt.subplots(1, 3, figsize=(18, 6), dpi=100)

    abs_q_cols_template = "AbsErr_Q"

    for ax_idx, (df, meta, label) in enumerate(regimes):
        ax = axes[ax_idx]

        # Get abs error quantile columns
        q_cols = [c for c in df.columns if c.startswith(abs_q_cols_template)]
        # Should be 7: -3σ, -2σ, -1σ, median, +1σ, +2σ, +3σ

        true_freq = df["TrueFreq"].values.astype(float)
        mask = true_freq > 0
        x = true_freq[mask]

        # Convert error quantiles to estimated frequency: est = true_freq + error
        est_cols = []
        for col in q_cols:
            est_name = col.replace("AbsErr_Q", "Est_Q")
            df[est_name] = df["TrueFreq"] + df[col]
            est_cols.append(est_name)

        error_bound = float(meta.get("error_bound", "nan"))
        bound_color = "#2166ac"

        ax.set_xscale("log")
        ax.set_yscale("log")

        # Lower bound: y = x (true frequency, no error)
        ax.plot(x, x, color=bound_color, linewidth=2.0, linestyle="--",
                alpha=0.9, label=r"Lower bound: $c_j$" if ax_idx == 0 else None,
                zorder=10)

        # Upper bound: y = x + εN
        if not np.isnan(error_bound):
            ax.plot(x, x + error_bound, color=bound_color, linewidth=2.0,
                    linestyle="-.", alpha=0.9,
                    label=r"Upper bound: $c_j + \varepsilon N$" if ax_idx == 0 else None,
                    zorder=10)

        # Shaded bands (outer to inner)
        # Indices: 0=-3σ, 1=-2σ, 2=-1σ, 3=median, 4=+1σ, 5=+2σ, 6=+3σ
        band_specs = [
            (0, 6, "#dadaeb", 0.5, r"$\pm 3\sigma$ (Q0.1%/Q99.9%)"),
            (1, 5, "#bcbddc", 0.6, r"$\pm 2\sigma$ (Q2.3%/Q97.7%)"),
            (2, 4, "#9e9ac8", 0.7, r"$\pm 1\sigma$ (Q15.9%/Q84.1%)"),
        ]
        for lo, hi, color, alpha, blabel in band_specs:
            lo_vals = df[est_cols[lo]].values[mask].astype(float)
            hi_vals = df[est_cols[hi]].values[mask].astype(float)
            ax.fill_between(x, lo_vals, hi_vals, color=color, alpha=alpha,
                            label=blabel if ax_idx == 0 else None)

        # Median line
        med_vals = df[est_cols[3]].values[mask].astype(float)
        ax.plot(x, med_vals, color="#e31a1c", linewidth=1.5,
                label="Median estimate" if ax_idx == 0 else None)

        zipf_exp = meta.get("zipf_exponent", "?")
        viol_frac = meta.get("actual_violation_frac", "?")
        delta = meta.get("theoretical_delta", "?")
        ax.set_title(rf"Zipf $\alpha$ = {zipf_exp}")
        ax.set_xlabel("True frequency (log scale)")
        ax.grid(True, which="major", alpha=0.3, linewidth=0.8)
        ax.grid(True, which="minor", alpha=0.15, linewidth=0.5, linestyle=":")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        # Per-panel violation annotation
        ax.plot([], [], ' ',
                label=f"Violations: {float(viol_frac):.4%}" if ax_idx == 0 else None)

    axes[0].set_ylabel("Estimated frequency (log scale)")

    # Shared legend
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=4, frameon=True,
               fancybox=False, framealpha=0.95, bbox_to_anchor=(0.5, 1.02))

    fig.suptitle("CMS Point Query: Estimated Frequency Profile", fontweight="bold", y=1.06)
    plt.tight_layout()

    logging.info(f"Saving {output_file}...")
    fig.savefig(output_file, format="svg", bbox_inches="tight")
    logging.info(f"Done: {output_file}")


def plot_relative_error(regimes, output_file):
    """
    Plot relative error vs true frequency — shows the practitioner story:
    heavy hitters have tiny relative error, rare items have huge relative error.
    """
    plt.style.use("seaborn-v0_8-paper")
    plt.rcParams.update({
        "font.size": 10,
        "axes.labelsize": 11,
        "axes.titlesize": 12,
        "legend.fontsize": 8,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
    })

    fig, axes = plt.subplots(1, 3, figsize=(18, 6), dpi=100, sharey=True)

    rel_q_cols_template = "RelErr_Q"
    band_specs = [
        (0, 6, "#dadaeb", 0.5, r"$\pm 3\sigma$"),
        (1, 5, "#bcbddc", 0.6, r"$\pm 2\sigma$"),
        (2, 4, "#9e9ac8", 0.7, r"$\pm 1\sigma$"),
    ]

    for ax_idx, (df, meta, _) in enumerate(regimes):
        ax = axes[ax_idx]
        q_cols = [c for c in df.columns if c.startswith(rel_q_cols_template)]

        # Group by TrueFreq and average the quantiles within each group
        # to eliminate duplicate x-values that break fill_between
        df_pos = df[df["TrueFreq"] > 0].copy()
        grouped = df_pos.groupby("TrueFreq")[q_cols].mean().sort_index()
        x = grouped.index.values.astype(float)

        ax.set_xscale("log")
        ax.set_yscale("log")

        for lo, hi, color, alpha, blabel in band_specs:
            lo_vals = np.maximum(grouped[q_cols[lo]].values.astype(float), 1e-10)
            hi_vals = np.maximum(grouped[q_cols[hi]].values.astype(float), 1e-10)
            ax.fill_between(x, lo_vals, hi_vals, color=color, alpha=alpha,
                            label=blabel if ax_idx == 0 else None)

        med_vals = np.maximum(grouped[q_cols[3]].values.astype(float), 1e-10)
        ax.plot(x, med_vals, color="#e31a1c", linewidth=1.5,
                label="Median" if ax_idx == 0 else None)

        # Reference line at rel_err = 1 (error equals true count)
        ax.axhline(y=1.0, color="#2166ac", linewidth=1.5, linestyle="--",
                   alpha=0.8, label="Rel error = 1" if ax_idx == 0 else None)

        # Theoretical relative bound: εN / c_j
        error_bound = float(meta.get("error_bound", "nan"))
        if not np.isnan(error_bound):
            ax.plot(x, error_bound / x, color="#2166ac", linewidth=1.5,
                    linestyle="-.", alpha=0.8,
                    label=r"$\varepsilon N / c_j$" if ax_idx == 0 else None)

        zipf_exp = meta.get("zipf_exponent", "?")
        ax.set_title(rf"Zipf $\alpha$ = {zipf_exp}")
        ax.set_xlabel("True frequency (log scale)")
        ax.grid(True, which="major", alpha=0.3, linewidth=0.8)
        ax.grid(True, which="minor", alpha=0.15, linewidth=0.5, linestyle=":")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

    axes[0].set_ylabel("Relative error (log scale)")

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=5, frameon=True,
               fancybox=False, framealpha=0.95, bbox_to_anchor=(0.5, 1.02))

    fig.suptitle("CMS Relative Error vs True Frequency", fontweight="bold", y=1.06)
    plt.tight_layout()

    logging.info(f"Saving {output_file}...")
    fig.savefig(output_file, format="svg", bbox_inches="tight")
    logging.info(f"Done: {output_file}")


def main():
    if len(sys.argv) < 4:
        print(
            "Usage: plot_cms_point_query.py high_skew.tsv medium_skew.tsv low_skew.tsv",
            file=sys.stderr,
        )
        sys.exit(1)

    files = sys.argv[1:4]
    regime_labels = ["High skew", "Medium skew", "Low skew"]

    regimes = []
    for filepath, label in zip(files, regime_labels):
        logging.info(f"Loading {filepath}...")
        df, meta = load_regime(filepath)
        logging.info(f"  {len(df)} items, zipf_exponent={meta.get('zipf_exponent', '?')}")
        regimes.append((df, meta, label))

    plot_estimated_frequency(regimes, "cms_point_query_error.svg")
    plot_relative_error(regimes, "cms_rel_error_vs_freq_rank.svg")


if __name__ == "__main__":
    main()
