# DataSketches Count-Min Sketch Characterisation

## Project Overview

This repository provides comprehensive benchmarking and performance characterisation for 
Apache DataSketches implementations.
Our focus will be on the Apache DataSketches C++ implementation ONLY.

**Primary Goals:**
1. Build robust benchmarking infrastructure for Count-Min Sketch
2. Establish baseline performance metrics before optimisations
3. Enable comparative analysis of different implementations and optimisations
4. Generate reproducible performance reports in python.  You MUST use uv package management in python.

**Related Work:**
- Main implementation: https://github.com/apache/datasketches-cpp
- Tracking issue: https://github.com/apache/datasketches-cpp/milestone/1
- Further context available on the "Milestone": https://github.com/apache/datasketches-cpp/milestone/1

---

## Phase 0 - Baseline Characterisation

We are in the initial benchmarking infrastructure phase.
Start with a small experiment, ensure the infrastructure works, then gradually scale up.
The task is to build a point query profile of the Count Min Sketch.
Reuse existing patterns within this repository where possible (job_profile base class,
registration in main.cpp, KLL quantile sketches for distributional tracking).

## Experiment 1 — Point Query Error Profile

Build a C++ profile (registered in main.cpp as a job_profile) that characterises
the distribution of CMS point query absolute error across many trials.

### Design

1. **Fix the stream once.** Generate a single stream of length N from a Zipf(universe_size, exponent)
   distribution. Cache the stream and the true counts c_j for every item j. The stream is reused
   identically across all trials — only the CMS hash seed varies per trial.

2. **Per-item KLL sketches.** Maintain one KLL quantile sketch Q_j per item j. These accumulate
   the absolute error (f_j - c_j) across all trials, giving the empirical error distribution.

3. **Trial loop.** For each trial t = 0 .. (1 << lgTrials) - 1:
   - Create a fresh CMS with a different seed (e.g. 42 + t*1000)
   - Insert the cached stream into the CMS
   - For every item j, query the CMS estimate f_j, compute error = f_j - c_j,
     and update Q_j with this error value
   - Track bound violations: count queries where error > epsilon * N

### Output format

- **stdout -> TSV.** Metadata as `# key=value` comment lines (error_bound, theoretical_delta,
  actual_violation_frac, zipf_exponent, etc.), then a header row, then one row per item sorted
  by ascending true frequency. Columns: Item, TrueFreq, then one column per quantile level.
- **stderr -> diagnostics.** Parameters, progress every ~10% of trials, summary statistics.

### Quantile levels

Output dense quantile levels, especially in the upper tail where CMS error is concentrated
(CMS only overestimates, so the distribution is non-negative and right-skewed):

- Below median: 0.0 (min), 0.00135, 0.02275, 0.15866, 0.5
- Upper body: 0.84134, 0.90, 0.91, 0.92, ..., 0.97
- Upper tail: 0.97725, 0.98, 0.99, 0.991, ..., 0.999
- Extreme tail: 0.99865, 0.9999, 1.0 (max)

Note: the levels 0.00135, 0.02275, 0.15866, 0.5, 0.84134, 0.97725, 0.99865 correspond to
standard normal sigma levels and are useful reference points for the main band plot, but
CMS error is NOT normally distributed — do not label them as +/-sigma in plots.

### Plot 1 — Frequency profile (band plot)

Python script using PEP 723 inline metadata, run via `uv run script.py` (not `uv run python`).

- Log-log axes
- x-axis: true frequency (ascending), y-axis: estimated frequency (error quantile + true freq)
- Shaded bands from min/max (lightest) through sigma-level pairs to median (darkest)
- Median estimate as a distinct coloured line
- Lower bound line: y = x (true frequency, no error)
- Upper bound line: y = x + epsilon*N (theoretical worst case)
- Legend includes theoretical delta and actual bound violation fraction
- Zipf exponent in title

### Plot 2 — Error CDF at selected frequencies (slice plot)

For user-specified true frequencies, plot the empirical CDF of absolute error:

- x-axis: absolute error (log scale)
- y-axis: quantile level (cumulative probability, 0% to 100%)
- One curve per selected frequency, overlaid
- Vertical line at epsilon*N (theoretical bound)
- Dense upper-tail quantiles make the tail shape visible

### Build

Provide a Makefile with targets: `build`, `run` (build + profile), `plot` (regenerate from
existing TSV), and a combined target that builds, runs, and plots.