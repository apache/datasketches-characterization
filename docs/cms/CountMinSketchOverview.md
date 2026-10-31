---
layout: doc_page
---

# Count-Min Sketch Overview

The Count-Min Sketch (CMS) is a probabilistic data structure for estimating the frequency
of items in a data stream using sub-linear space. It was introduced by
Cormode and Muthukrishnan (2005).

## How It Works

A CMS consists of a `d × w` array of counters (depth × width), initialized to zero.
Each of the `d` rows is associated with an independent hash function mapping items
to columns in `[0, w)`.

- **Update(item, count):** For each row `i`, hash the item to column `h_i(item)` and
  increment the counter at position `(i, h_i(item))` by `count`.
- **Estimate(item):** Return the minimum counter value across all `d` rows:
  `f̂(item) = min_i counter[i][h_i(item)]`.

## Theoretical Guarantees

For a CMS with width `w` and depth `d`, querying any item yields:

- **Upper bound:** `f̂(item) >= f(item)` — the estimate is always at least the true count
  (CMS never underestimates for non-negative updates).
- **Error bound:** `f̂(item) <= f(item) + ε·N` with probability at least `1 - δ`, where:
  - `ε = e / w` (e = Euler's number ≈ 2.718)
  - `δ = e^(-d)`
  - `N` is the total stream weight

## Parameter Selection

The DataSketches library provides helper functions:

```cpp
#include <count_min.hpp>

// For epsilon=0.01 (1% of stream weight error bound):
uint32_t width = count_min_sketch<int64_t>::suggest_num_buckets(0.01);  // → 272

// For 95% confidence (delta=0.05):
uint8_t depth = count_min_sketch<int64_t>::suggest_num_hashes(0.95);    // → 4
```

The resulting sketch uses `width × depth × 8` bytes for 64-bit counters, plus a small
constant overhead. With the default parameters above, this is roughly **8.5 KB** regardless
of the number of distinct items or stream length.

## When to Use CMS vs Frequent Items

| Criterion | Count-Min Sketch | Frequent Items |
|-----------|-----------------|----------------|
| Query type | Point query for any item | Only returns items above threshold |
| Error guarantee | Additive: `ε·N` | Relative to tail weight |
| Heavy hitter accuracy | Excellent (error ≪ true count) | Excellent |
| Long tail accuracy | Poor (error may dominate) | N/A (items not returned) |
| Mergeability | Yes (element-wise sum) | Yes |
| Space | Fixed `O(1/ε · log(1/δ))` | Fixed `O(1/ε)` |

**Rule of thumb:** Use CMS when you need to estimate the frequency of *arbitrary* items
(including items you haven't seen). Use Frequent Items when you only care about identifying
and counting the heavy hitters.

## References

- G. Cormode and S. Muthukrishnan. "An Improved Data Stream Summary: The Count-Min Sketch
  and its Applications." *Journal of Algorithms*, 55(1):58–75, 2005.
- [DataSketches CMS documentation](https://datasketches.apache.org/docs/CountMin/CountMinSketch.html)
