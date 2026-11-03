---
layout: doc_page
---

# Count-Min Sketch: C++ Examples

## Basic Usage

```cpp
#include <count_min.hpp>

using namespace datasketches;

// Create a CMS with target accuracy epsilon=0.01, confidence=0.95
uint32_t width = count_min_sketch<int64_t>::suggest_num_buckets(0.01);
uint8_t depth = count_min_sketch<int64_t>::suggest_num_hashes(0.95);
uint64_t seed = 42;

count_min_sketch<int64_t> sketch(depth, width, seed);

// Update: add items to the sketch
sketch.update(1);          // item=1, implicit count=1
sketch.update(2);
sketch.update(1);
sketch.update(3, 5);       // item=3, count=5

// Query: estimate the frequency of an item
int64_t est_1 = sketch.get_estimate(1);  // >= 2
int64_t est_2 = sketch.get_estimate(2);  // >= 1
int64_t est_3 = sketch.get_estimate(3);  // >= 5
int64_t est_4 = sketch.get_estimate(4);  // >= 0 (never inserted)

// Total stream weight
int64_t total = sketch.get_total_weight();  // 9
```

## Merging Two Sketches

Sketches with the **same seed, width, and depth** can be merged:

```cpp
count_min_sketch<int64_t> sketch_a(depth, width, seed);
count_min_sketch<int64_t> sketch_b(depth, width, seed);

// Populate independently
sketch_a.update(1, 100);
sketch_b.update(1, 200);
sketch_b.update(2, 50);

// Merge b into a
sketch_a.merge(sketch_b);

// Now sketch_a reflects the combined stream
int64_t est = sketch_a.get_estimate(1);  // >= 300
```

## Serialization

```cpp
// Serialize to bytes
auto bytes = sketch.serialize();

// Deserialize
auto recovered = count_min_sketch<int64_t>::deserialize(
    bytes.data(), bytes.size(), seed);

// Estimates are identical after round-trip
assert(recovered.get_estimate(1) == sketch.get_estimate(1));
```

## Understanding the Error Bound

```cpp
double epsilon = std::exp(1.0) / width;
double N = static_cast<double>(sketch.get_total_weight());
double error_bound = epsilon * N;

int64_t est = sketch.get_estimate(item);
int64_t true_count = /* known from external source */;

// With probability >= 1 - delta:
//   true_count <= est <= true_count + error_bound
//
// CMS never underestimates (for non-negative updates).
// The overestimate is at most epsilon * N with high probability.
```
