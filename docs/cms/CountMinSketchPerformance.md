---
layout: doc_page
---

# Count-Min Sketch: Accuracy Characterization

This page presents empirical accuracy results for the Count-Min Sketch (CMS)
point query operation across different Zipfian skew settings.

## Experiment Setup

- **Universe size:** U = 8,192 distinct items
- **Stream length:** N = 1,048,576 (2^20)
- **CMS parameters:** ε = 0.01, confidence = 0.95 (width = 272, depth = 4)
- **Trials:** 8,192 (only the CMS hash seed varies per trial; the stream is fixed)
- **Error aggregation:** Per-item KLL quantile sketches (k = 200) across all trials
- **Skew regimes:** Zipf α ∈ {1.5, 1.0, 0.5}

## Absolute Error vs Frequency Rank

Items are ranked by true frequency (rank 1 = most frequent). The bands show the
cross-trial distribution of absolute error (`estimate - true_count`) at 7 sigma levels.

![CMS Absolute Error vs Frequency Rank](../../cms_abs_error_vs_freq_rank.svg)

**Key observations:**

- The absolute error is roughly constant across all frequency ranks — it depends on the
  *total stream weight* N, not the individual item's frequency.
- The theoretical bound `ε·N` (dashed line) holds with high probability across all items.
- Higher Zipf skew concentrates more weight on fewer items, but the absolute error
  magnitude is determined by stream length, not per-item frequency.

## Relative Error vs Frequency Rank

The same data shown as relative error (`abs_error / true_count`):

![CMS Relative Error vs Frequency Rank](../../cms_rel_error_vs_freq_rank.svg)

**Key observations:**

- **Heavy hitters (low rank)** have tiny relative error — the additive `ε·N` error is
  negligible compared to their large true counts.
- **Long-tail items (high rank)** have enormous relative error — the fixed additive error
  dominates their small true counts.
- This is the fundamental CMS trade-off: excellent for heavy hitters, poor for rare items.
- Higher skew (α = 1.5) exaggerates this effect because the frequency distribution is
  more concentrated.

## Bound Violation Rate

The CMS theoretical guarantee states that `P(error > ε·N) ≤ δ`. The measured violation
rates across all items and trials are reported in each TSV file's metadata. Typical values
are well below the theoretical δ = 0.05.

## Data Files

Raw TSV results with full quantile data are available in `cpp/results/`:

- `cms_point_query_high_skew.tsv` (α = 1.5)
- `cms_point_query_medium_skew.tsv` (α = 1.0)
- `cms_point_query_low_skew.tsv` (α = 0.5)
