#!/usr/bin/env python3
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "matplotlib",
#     "numpy",
# ]
# ///
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Plot CMS point query error quantiles from the cms-frequency profile output.

Reads a single-row TSV (header + one data line) and produces a bar chart of
the absolute error quantile distribution with the theoretical bound overlaid.

Usage:
    python plot_cms_frequency.py <input.tsv> [output.svg]

Expected TSV columns (tab-separated, one data row):
    Width  Depth  Trials  Distinct  StreamLen  TheoreticalBound
    MeanAbsErr  MedianAbsErr  P75AbsErr  P90AbsErr  P95AbsErr  MaxAbsErr
    FracExceedingBound
"""

import sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <input.tsv> [output.svg]")
        sys.exit(1)

    tsv_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "cms_frequency_error.svg"

    with open(tsv_path) as f:
        header = f.readline().strip().split("\t")
        values = f.readline().strip().split("\t")

    row = dict(zip(header, values))

    width = int(row["Width"])
    depth = int(row["Depth"])
    trials = int(row["Trials"])
    distinct = int(row["Distinct"])
    stream_len = int(row["StreamLen"])
    theo_bound = float(row["TheoreticalBound"])
    frac_exceed = float(row["FracExceedingBound"])

    quantile_labels = ["Mean", "Median", "P75", "P90", "P95", "Max"]
    quantile_keys = [
        "MeanAbsErr", "MedianAbsErr", "P75AbsErr",
        "P90AbsErr", "P95AbsErr", "MaxAbsErr",
    ]
    quantile_values = [float(row[k]) for k in quantile_keys]

    fig, ax = plt.subplots(figsize=(8, 5))

    x = np.arange(len(quantile_labels))
    bars = ax.bar(x, quantile_values, color="steelblue", edgecolor="black", linewidth=0.5)

    ax.axhline(y=theo_bound, color="red", linestyle="--", linewidth=2,
               label=f"Theoretical bound (e/w * N = {theo_bound:.1f})")

    ax.set_xticks(x)
    ax.set_xticklabels(quantile_labels)
    ax.set_ylabel("Absolute Error")
    ax.set_title(
        f"CMS Point Query Error Distribution\n"
        f"width={width}, depth={depth}, distinct={distinct}, "
        f"stream={stream_len}, trials={trials}"
    )
    ax.legend()

    ax.annotate(
        f"Frac exceeding bound: {frac_exceed:.4f}",
        xy=(0.98, 0.92), xycoords="axes fraction",
        ha="right", fontsize=9,
        bbox=dict(boxstyle="round,pad=0.3", fc="lightyellow", ec="gray"),
    )

    fig.tight_layout()
    fig.savefig(output_path, bbox_inches="tight")
    print(f"Saved: {output_path}")


if __name__ == "__main__":
    main()
