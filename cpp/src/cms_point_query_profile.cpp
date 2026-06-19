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
#include <fstream>
#include <algorithm>
#include <vector>
#include <cmath>
#include <iomanip>
#include <string>

#include <count_min.hpp>
#include <kll_sketch.hpp>

#include "cms_point_query_profile.hpp"
#include "zipf_distribution.hpp"

namespace datasketches {

typedef count_min_sketch<int64_t> cms_int64;

void cms_point_query_profile::run() {
  const unsigned lg_trials = 13;
  const size_t num_trials = 1ULL << lg_trials;   // 8192
  const unsigned universe_size = 1 << 13;         // U = 8192
  const size_t stream_length = 1ULL << 20;        // N = 1,048,576

  // CMS parameters via suggest functions
  const double eps_param = 0.01;
  const double conf_param = 0.95;
  const uint32_t num_buckets = cms_int64::suggest_num_buckets(eps_param);
  const uint8_t num_hashes = cms_int64::suggest_num_hashes(conf_param);
  const double epsilon = std::exp(1.0) / num_buckets;
  const double error_bound = epsilon * static_cast<double>(stream_length);
  const double theoretical_delta = 1.0 - conf_param;

  const uint16_t kll_k = 200;

  // 7 sigma-level quantiles for cross-trial aggregation
  const double quantiles[] = {
    0.00135, 0.02275, 0.15866, 0.5, 0.84134, 0.97725, 0.99865
  };
  const size_t num_quantiles = sizeof(quantiles) / sizeof(quantiles[0]);

  // Three skew regimes
  const double skew_exponents[] = {1.5, 1.0, 0.5};
  const char* skew_suffixes[] = {"high_skew", "medium_skew", "low_skew"};
  const char* skew_labels[] = {
    "High skew (alpha=1.5)",
    "Medium skew (alpha=1.0)",
    "Low skew (alpha=0.5)"
  };
  const size_t num_skews = 3;

  std::cerr << "# CMS Point Query Error Profile" << std::endl;
  std::cerr << "# lgTrials = " << lg_trials
            << " (" << num_trials << " trials)" << std::endl;
  std::cerr << "# Universe size U = " << universe_size << std::endl;
  std::cerr << "# Stream length N = " << stream_length << std::endl;
  std::cerr << "# num_buckets (width) = " << num_buckets << std::endl;
  std::cerr << "# num_hashes (depth) = "
            << static_cast<unsigned>(num_hashes) << std::endl;
  std::cerr << "# epsilon = e/width = " << std::fixed
            << std::setprecision(6) << epsilon << std::endl;
  std::cerr << "# error_bound (epsilon*N) = " << std::setprecision(2)
            << error_bound << std::endl;
  std::cerr << "# kll_k = " << kll_k << std::endl;
  std::cerr << "#" << std::endl;

  for (size_t cfg = 0; cfg < num_skews; cfg++) {
    const double zipf_exponent = skew_exponents[cfg];
    std::cerr << "# === " << skew_labels[cfg] << " ===" << std::endl;

    // --- Step 1: Generate stream ONCE, cache true counts ---
    std::cerr << "# Generating Zipf stream (alpha="
              << std::setprecision(1) << zipf_exponent << ")..." << std::endl;
    zipf_distribution zipf(universe_size, zipf_exponent);

    std::vector<int64_t> stream(stream_length);
    std::vector<int64_t> true_count(universe_size + 1, 0);

    for (size_t i = 0; i < stream_length; i++) {
      unsigned item = zipf.sample();
      stream[i] = static_cast<int64_t>(item);
      true_count[item]++;
    }

    unsigned num_distinct = 0;
    for (unsigned j = 1; j <= universe_size; j++) {
      if (true_count[j] > 0) num_distinct++;
    }
    std::cerr << "# Distinct items: " << num_distinct << std::endl;

    // --- Step 2: Per-item KLL sketches for abs and rel error ---
    std::vector<kll_sketch<double> > abs_error_sketches;
    std::vector<kll_sketch<double> > rel_error_sketches;
    abs_error_sketches.reserve(universe_size + 1);
    rel_error_sketches.reserve(universe_size + 1);
    for (unsigned j = 0; j <= universe_size; j++) {
      abs_error_sketches.push_back(kll_sketch<double>(kll_k));
      rel_error_sketches.push_back(kll_sketch<double>(kll_k));
    }

    // Per-item bound violation counters
    std::vector<size_t> item_violations(universe_size + 1, 0);
    size_t total_queries = 0;
    size_t total_violations = 0;

    // --- Step 3: Run trials ---
    for (size_t trial = 0; trial < num_trials; trial++) {
      const uint64_t trial_seed = 42 + trial * 1000;

      cms_int64 sketch(num_hashes, num_buckets, trial_seed);

      for (size_t i = 0; i < stream_length; i++) {
        sketch.update(stream[i]);
      }

      double trial_max_err = 0;
      size_t trial_violations = 0;
      for (unsigned j = 1; j <= universe_size; j++) {
        if (true_count[j] == 0) continue;

        int64_t f_j = sketch.get_estimate(static_cast<int64_t>(j));
        double abs_err = static_cast<double>(f_j - true_count[j]);
        double rel_err = abs_err / static_cast<double>(true_count[j]);

        abs_error_sketches[j].update(abs_err);
        rel_error_sketches[j].update(rel_err);

        total_queries++;
        if (abs_err > error_bound) {
          total_violations++;
          trial_violations++;
          item_violations[j]++;
        }
        if (abs_err > trial_max_err) trial_max_err = abs_err;
      }

      if (trial == 0 || (trial + 1) % std::max(num_trials / 10, (size_t)1) == 0) {
        std::cerr << "# trial " << (trial + 1) << "/" << num_trials
                  << "  max_abs_error=" << std::fixed << std::setprecision(0)
                  << trial_max_err
                  << "  violations=" << trial_violations << std::endl;
      }
    }

    // --- Step 4: Sort items by true frequency descending, assign ranks ---
    struct item_result {
      unsigned item_id;
      int64_t freq;
    };
    std::vector<item_result> results;
    results.reserve(num_distinct);
    for (unsigned j = 1; j <= universe_size; j++) {
      if (true_count[j] > 0) {
        item_result r;
        r.item_id = j;
        r.freq = true_count[j];
        results.push_back(r);
      }
    }
    std::sort(results.begin(), results.end(),
        [](const item_result& a, const item_result& b) {
          return a.freq > b.freq;  // descending: rank 1 = most frequent
        });

    // --- Step 5: Write TSV to file ---
    const double actual_violation_frac =
        (total_queries > 0)
        ? static_cast<double>(total_violations) / static_cast<double>(total_queries)
        : 0.0;

    std::string filename = std::string("cpp/results/cms_point_query_")
                           + skew_suffixes[cfg] + ".tsv";
    std::ofstream out(filename);
    if (!out.is_open()) {
      std::cerr << "# ERROR: could not open " << filename << std::endl;
      continue;
    }

    // Metadata comment lines
    out << "# error_bound=" << std::fixed << std::setprecision(2)
        << error_bound << std::endl;
    out << "# theoretical_delta=" << std::setprecision(4)
        << theoretical_delta << std::endl;
    out << "# actual_violation_frac=" << std::setprecision(6)
        << actual_violation_frac << std::endl;
    out << "# zipf_exponent=" << std::setprecision(1)
        << zipf_exponent << std::endl;
    out << "# universe_size=" << universe_size << std::endl;
    out << "# stream_length=" << stream_length << std::endl;
    out << "# num_trials=" << num_trials << std::endl;
    out << "# num_buckets=" << num_buckets << std::endl;
    out << "# num_hashes=" << static_cast<unsigned>(num_hashes) << std::endl;
    out << "# epsilon=" << std::setprecision(6) << epsilon << std::endl;

    // TSV header
    out << "FreqRank\tTrueFreq";
    for (size_t q = 0; q < num_quantiles; q++) {
      out << "\tAbsErr_Q" << std::fixed << std::setprecision(5) << quantiles[q];
    }
    for (size_t q = 0; q < num_quantiles; q++) {
      out << "\tRelErr_Q" << std::fixed << std::setprecision(5) << quantiles[q];
    }
    out << "\tBoundViolationRate" << std::endl;

    for (size_t i = 0; i < results.size(); i++) {
      unsigned j = results[i].item_id;
      out << (i + 1) << "\t" << results[i].freq;

      for (size_t q = 0; q < num_quantiles; q++) {
        out << "\t" << std::fixed << std::setprecision(2)
            << abs_error_sketches[j].get_quantile(quantiles[q]);
      }
      for (size_t q = 0; q < num_quantiles; q++) {
        out << "\t" << std::fixed << std::setprecision(6)
            << rel_error_sketches[j].get_quantile(quantiles[q]);
      }

      double viol_rate = static_cast<double>(item_violations[j])
                         / static_cast<double>(num_trials);
      out << "\t" << std::setprecision(6) << viol_rate << std::endl;
    }

    out.close();

    // Summary to stderr
    std::cerr << "#" << std::endl;
    std::cerr << "# Wrote " << results.size() << " items to " << filename << std::endl;
    std::cerr << "# Bound violations: " << total_violations
              << " / " << total_queries
              << " (" << std::setprecision(4)
              << 100.0 * actual_violation_frac << "%)" << std::endl;
    std::cerr << "#" << std::endl;
  }

  std::cerr << "# Done." << std::endl;
}

} /* namespace datasketches */
