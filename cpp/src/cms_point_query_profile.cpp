/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <iostream>
#include <algorithm>
#include <vector>
#include <cmath>
#include <iomanip>

#include <count_min.hpp>
#include <kll_sketch.hpp>

#include "cms_point_query_profile.hpp"
#include "zipf_distribution.hpp"

namespace datasketches {

typedef count_min_sketch<int64_t> cms_int64;

void cms_point_query_profile::run() {
  // Experiment parameters (small for verification; scale up later)
  const unsigned lg_trials = 13;
  const size_t num_trials = 1ULL << lg_trials;  
  const unsigned universe_size = 1 << 10;        
  const size_t stream_length = 1ULL << 16;       
  const double zipf_exponent = 0.7;

  // CMS parameters
  const uint32_t num_buckets = cms_int64::suggest_num_buckets(0.01);
  const uint8_t num_hashes = cms_int64::suggest_num_hashes(0.95);
  const double epsilon = std::exp(1.0) / num_buckets;

  // KLL parameters
  const uint16_t kll_k = 200;

  // Output quantiles: sparse below median, dense in the upper tail
  // CMS only overestimates so the interesting behavior is all in the right tail.
  // Includes the 7 sigma levels for the main plot band selection.
  const double quantiles[] = {
    0.0, 0.00135, 0.02275, 0.15866, 0.5,                      // min + lower sigma levels
    0.84134, 0.9, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97,  // upper body
    0.97725, 0.98, 0.99,                                        // +2σ region
    0.991, 0.992, 0.993, 0.994, 0.995, 0.996, 0.997, 0.998, 0.999, // upper tail
    0.99865, 0.9999, 1.0                                        // +3σ, near-max, max
  };
  const size_t num_quantiles = sizeof(quantiles) / sizeof(quantiles[0]);

  // Metadata to stderr for sanity checking
  std::cerr << "# CMS Point Query Error Profile (Experiment 1)" << std::endl;
  std::cerr << "# lgTrials = " << lg_trials
            << " (" << num_trials << " trials)" << std::endl;
  std::cerr << "# Universe size = " << universe_size << std::endl;
  std::cerr << "# Stream length N = " << stream_length << std::endl;
  std::cerr << "# num_buckets (width) = " << num_buckets << std::endl;
  std::cerr << "# num_hashes (depth) = "
            << static_cast<unsigned>(num_hashes) << std::endl;
  std::cerr << "# epsilon = e/width = " << std::fixed
            << std::setprecision(6) << epsilon << std::endl;
  std::cerr << "# Theoretical error bound epsilon*N = "
            << std::setprecision(2) << epsilon * stream_length << std::endl;
  std::cerr << "# zipf_exponent = " << zipf_exponent << std::endl;
  std::cerr << "# kll_k = " << kll_k << std::endl;
  std::cerr << "#" << std::endl;

  // --- Step 1: Generate the stream ONCE and cache true counts ---
  std::cerr << "# Generating fixed stream from Zipf distribution..." << std::endl;
  zipf_distribution zipf(universe_size, zipf_exponent);

  std::vector<int64_t> stream(stream_length);
  std::vector<int64_t> true_count(universe_size + 1, 0);  // c_j for each item j

  for (size_t i = 0; i < stream_length; i++) {
    unsigned item = zipf.sample();
    stream[i] = static_cast<int64_t>(item);
    true_count[item]++;
  }

  // Count distinct items that appeared
  unsigned num_distinct = 0;
  for (unsigned j = 1; j <= universe_size; j++) {
    if (true_count[j] > 0) num_distinct++;
  }
  std::cerr << "# Distinct items in stream: " << num_distinct << std::endl;
  std::cerr << "#" << std::endl;

  // --- Step 2: Per-item KLL sketches for absolute error ---
  std::vector<kll_sketch<double> > error_sketches;
  error_sketches.reserve(universe_size + 1);
  for (unsigned j = 0; j <= universe_size; j++) {
    error_sketches.push_back(kll_sketch<double>(kll_k));
  }

  // --- Step 3: Run trials ---
  const double error_bound = epsilon * static_cast<double>(stream_length);
  size_t total_queries = 0;
  size_t bound_violations = 0;

  for (size_t trial = 0; trial < num_trials; trial++) {
    const uint64_t trial_seed = 42 + trial * 1000;

    // 3a: Fresh CMS with different hash functions each trial
    cms_int64 sketch(num_hashes, num_buckets, trial_seed);

    // 3b: Insert the SAME cached stream
    for (size_t i = 0; i < stream_length; i++) {
      sketch.update(stream[i]);
    }

    // 3c & 3d: Query each item j, compute error, update KLL sketch
    double trial_max_err = 0;
    size_t trial_violations = 0;
    for (unsigned j = 1; j <= universe_size; j++) {
      int64_t f_j = sketch.get_estimate(static_cast<int64_t>(j));
      double abs_error = static_cast<double>(f_j - true_count[j]);
      error_sketches[j].update(abs_error);
      total_queries++;
      if (abs_error > error_bound) {
        bound_violations++;
        trial_violations++;
      }
      if (abs_error > trial_max_err) trial_max_err = abs_error;
    }

    // Periodic sanity-check output
    if (trial == 0 || (trial + 1) % std::max(num_trials / 10, (size_t)1) == 0) {
      std::cerr << "# trial " << (trial + 1) << "/" << num_trials
                << "  total_weight=" << sketch.get_total_weight()
                << "  max_abs_error=" << std::fixed << std::setprecision(0)
                << trial_max_err
                << "  violations=" << trial_violations
                << "/" << universe_size << std::endl;
    }
  }

  // --- Output: sort items by true frequency ascending ---
  struct item_result {
    unsigned item_id;
    int64_t freq;
  };
  std::vector<item_result> results;
  results.reserve(universe_size);
  for (unsigned j = 1; j <= universe_size; j++) {
    item_result r;
    r.item_id = j;
    r.freq = true_count[j];
    results.push_back(r);
  }
  std::sort(results.begin(), results.end(),
      [](const item_result& a, const item_result& b) {
        return a.freq < b.freq;
      });

  // TSV metadata as comment lines (parsed by plotting script)
  const double theoretical_delta = 0.05;  // 1 - confidence used in suggest_num_hashes
  const double actual_violation_frac =
      static_cast<double>(bound_violations) / static_cast<double>(total_queries);
  std::cout << "# error_bound=" << std::fixed << std::setprecision(2)
            << error_bound << std::endl;
  std::cout << "# theoretical_delta=" << std::setprecision(4)
            << theoretical_delta << std::endl;
  std::cout << "# actual_violation_frac=" << std::setprecision(6)
            << actual_violation_frac << std::endl;
  std::cout << "# bound_violations=" << bound_violations << std::endl;
  std::cout << "# total_queries=" << total_queries << std::endl;
  std::cout << "# zipf_exponent=" << std::setprecision(2)
            << zipf_exponent << std::endl;

  // TSV header
  std::cout << "Item\tTrueFreq";
  for (size_t q = 0; q < num_quantiles; q++) {
    std::cout << "\tQ_" << std::fixed << std::setprecision(5) << quantiles[q];
  }
  std::cout << std::endl;

  // Per-item output
  for (size_t i = 0; i < results.size(); i++) {
    unsigned j = results[i].item_id;
    std::cout << j
              << "\t" << results[i].freq;
    for (size_t q = 0; q < num_quantiles; q++) {
      std::cout << "\t" << std::fixed << std::setprecision(2)
                << error_sketches[j].get_quantile(quantiles[q]);
    }
    std::cout << std::endl;
  }

  // Summary statistics to stderr
  double min_median = 1e18, max_median = 0, sum_median = 0;
  for (unsigned j = 1; j <= universe_size; j++) {
    double med = error_sketches[j].get_quantile(0.5);
    if (med < min_median) min_median = med;
    if (med > max_median) max_median = med;
    sum_median += med;
  }
  std::cerr << "#" << std::endl;
  std::cerr << "# Summary:" << std::endl;
  std::cerr << "#   Items written: " << universe_size << std::endl;
  std::cerr << "#   Median error range: ["
            << std::fixed << std::setprecision(2)
            << min_median << ", " << max_median << "]" << std::endl;
  std::cerr << "#   Mean of median errors: "
            << sum_median / universe_size << std::endl;
  std::cerr << "#   Theoretical bound (epsilon*N): "
            << std::setprecision(2) << error_bound << std::endl;
  std::cerr << "#   Bound violations: " << bound_violations
            << " / " << total_queries
            << " (" << std::setprecision(4)
            << 100.0 * static_cast<double>(bound_violations) / static_cast<double>(total_queries)
            << "%)" << std::endl;
  std::cerr << "# Done." << std::endl;
}

} /* namespace datasketches */
