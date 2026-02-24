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
#include <iomanip>
#include <unordered_map>
#include <vector>
#include <cmath>

#include <count_min.hpp>
#include <kll_sketch.hpp>

#include "count_min_sketch_frequency_profile.hpp"
#include "zipf_distribution.hpp"

namespace datasketches {

void count_min_sketch_frequency_profile::run() {
  // Sketch depth (number of hash functions), fixed across all width configurations
  const unsigned num_hashes = 5;

  // Width sweep: lg_width from 8 to 12 (widths 256 to 4096)
  const unsigned lg_min_width = 8;
  const unsigned lg_max_width = 12;

  // Trial count parameters (adaptive: more trials for smaller widths)
  const unsigned lg_max_trials = 14;
  const unsigned lg_min_trials = 10;

  // Zipfian distribution parameters
  const double zipf_exponent = 1.1;

  // Stream length = distinct_items * stream_multiplier
  // Load factor (distinct_items / width) is kept at ~4 so that all width
  // configurations operate in the same collision regime.  This means
  // lg_range = lg_width + 2 and stream_length scales with width.
  const unsigned stream_multiplier = 16;

  // KLL sketch parameter for tracking error distributions
  const uint16_t kll_k = 200;

  std::cout << "Width\tDepth\tTrials\tDistinct\tStreamLen\t"
            << "TheoreticalBound\t"
            << "MeanAbsErr\tMedianAbsErr\tP75AbsErr\tP90AbsErr\tP95AbsErr\tMaxAbsErr\t"
            << "MeanRelErr\tMedianRelErr\tP75RelErr\tP90RelErr\tP95RelErr\t"
            << "FracExceedingBound"
            << std::endl;

  for (unsigned lg_width = lg_min_width; lg_width <= lg_max_width; lg_width++) {
    const uint32_t width = 1 << lg_width;

    // lg_range = lg_width + 2 gives ~4x overload (distinct / width ≈ 4)
    const unsigned lg_range = lg_width + 2;
    const unsigned distinct_items = 1 << lg_range;
    const size_t stream_length = static_cast<size_t>(distinct_items) * stream_multiplier;

    // Theoretical error bound: epsilon * N where epsilon = e / width
    const double epsilon = std::exp(1.0) / width;
    const double theoretical_bound = epsilon * stream_length;

    const size_t num_trials = get_num_trials(
        width, 1 << lg_min_width, 1 << lg_max_width,
        lg_min_trials, lg_max_trials);

    zipf_distribution zipf(distinct_items, zipf_exponent);

    // Accumulators
    double sum_abs_error = 0;
    double sum_rel_error = 0;
    size_t total_exceeding_bound = 0;
    size_t total_items_queried = 0;

    // KLL sketches for error distribution quantiles
    kll_sketch<double> abs_error_sketch(kll_k);
    kll_sketch<double> rel_error_sketch(kll_k);

    for (size_t trial = 0; trial < num_trials; trial++) {
      // Generate Zipfian stream
      std::vector<uint64_t> stream(stream_length);
      for (size_t i = 0; i < stream_length; i++) {
        stream[i] = zipf.sample();
      }

      // Ground truth frequencies
      std::unordered_map<uint64_t, uint64_t> true_freq;
      for (size_t i = 0; i < stream_length; i++) {
        true_freq[stream[i]]++;
      }

      // Build CMS
      count_min_sketch<uint64_t> sketch(num_hashes, width);
      for (size_t i = 0; i < stream_length; i++) {
        sketch.update(stream[i]);
      }

      // Point-query every distinct item and record errors
      for (const auto& kv : true_freq) {
        const uint64_t estimate = sketch.get_estimate(kv.first);
        const double abs_error = static_cast<double>(estimate) - static_cast<double>(kv.second);
        const double rel_error = abs_error / static_cast<double>(kv.second);

        abs_error_sketch.update(abs_error);
        rel_error_sketch.update(rel_error);

        sum_abs_error += abs_error;
        sum_rel_error += rel_error;
        total_items_queried++;

        if (abs_error > theoretical_bound) {
          total_exceeding_bound++;
        }
      }
    }

    // Summary statistics
    const double mean_abs = sum_abs_error / total_items_queried;
    const double median_abs = abs_error_sketch.get_quantile(0.5);
    const double p75_abs = abs_error_sketch.get_quantile(0.75);
    const double p90_abs = abs_error_sketch.get_quantile(0.90);
    const double p95_abs = abs_error_sketch.get_quantile(0.95);
    const double max_abs = abs_error_sketch.get_max_item();

    const double mean_rel = sum_rel_error / total_items_queried;
    const double median_rel = rel_error_sketch.get_quantile(0.5);
    const double p75_rel = rel_error_sketch.get_quantile(0.75);
    const double p90_rel = rel_error_sketch.get_quantile(0.90);
    const double p95_rel = rel_error_sketch.get_quantile(0.95);

    const double frac_exceeding = static_cast<double>(total_exceeding_bound) /
                                  static_cast<double>(total_items_queried);

    std::cout << width << "\t"
              << num_hashes << "\t"
              << num_trials << "\t"
              << distinct_items << "\t"
              << stream_length << "\t"
              << theoretical_bound << "\t"
              << mean_abs << "\t"
              << median_abs << "\t"
              << p75_abs << "\t"
              << p90_abs << "\t"
              << p95_abs << "\t"
              << max_abs << "\t"
              << mean_rel << "\t"
              << median_rel << "\t"
              << p75_rel << "\t"
              << p90_rel << "\t"
              << p95_rel << "\t"
              << frac_exceeding
              << std::endl;
  }
}

} /* namespace datasketches */
