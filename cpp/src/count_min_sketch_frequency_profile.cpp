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
#include <unordered_map>
#include <vector>
#include <cmath>

#include <count_min.hpp>
#include <kll_sketch.hpp>

#include "count_min_sketch_frequency_profile.hpp"
#include "zipf_distribution.hpp"

namespace datasketches {

void count_min_sketch_frequency_profile::run() {
  // Sketch parameters
  const uint8_t num_hashes = 5;
  const uint32_t width = 1024;

  // Stream parameters
  const unsigned lg_distinct = 12;   // 4096 distinct items
  const unsigned distinct_items = 1 << lg_distinct;
  const size_t stream_length = distinct_items * 16;
  const double zipf_exponent = 1.1;

  // Experiment parameters
  const size_t num_trials = 1 << 12; // 4096 trials
  const uint16_t kll_k = 200;

  const double epsilon = std::exp(1.0) / width;
  const double theoretical_bound = epsilon * stream_length;

  zipf_distribution zipf(distinct_items, zipf_exponent);

  double sum_abs_error = 0;
  size_t total_exceeding_bound = 0;
  size_t total_items_queried = 0;
  kll_sketch<double> abs_error_sketch(kll_k);

  for (size_t trial = 0; trial < num_trials; trial++) {
    std::vector<uint64_t> stream(stream_length);
    for (size_t i = 0; i < stream_length; i++) {
      stream[i] = zipf.sample();
    }

    std::unordered_map<uint64_t, uint64_t> true_freq;
    for (size_t i = 0; i < stream_length; i++) {
      true_freq[stream[i]]++;
    }

    count_min_sketch<uint64_t> sketch(num_hashes, width);
    for (size_t i = 0; i < stream_length; i++) {
      sketch.update(stream[i]);
    }

    for (const auto& kv : true_freq) {
      const double abs_error = static_cast<double>(sketch.get_estimate(kv.first))
                             - static_cast<double>(kv.second);
      abs_error_sketch.update(abs_error);
      sum_abs_error += abs_error;
      total_items_queried++;
      if (abs_error > theoretical_bound) {
        total_exceeding_bound++;
      }
    }
  }

  const double frac_exceeding = static_cast<double>(total_exceeding_bound)
                              / static_cast<double>(total_items_queried);

  std::cout << "Width\tDepth\tTrials\tDistinct\tStreamLen\tTheoreticalBound\t"
            << "MeanAbsErr\tMedianAbsErr\tP75AbsErr\tP90AbsErr\tP95AbsErr\tMaxAbsErr\t"
            << "FracExceedingBound"
            << std::endl;

  std::cout << width << "\t"
            << static_cast<unsigned>(num_hashes) << "\t"
            << num_trials << "\t"
            << distinct_items << "\t"
            << stream_length << "\t"
            << theoretical_bound << "\t"
            << sum_abs_error / total_items_queried << "\t"
            << abs_error_sketch.get_quantile(0.5) << "\t"
            << abs_error_sketch.get_quantile(0.75) << "\t"
            << abs_error_sketch.get_quantile(0.90) << "\t"
            << abs_error_sketch.get_quantile(0.95) << "\t"
            << abs_error_sketch.get_max_item() << "\t"
            << frac_exceeding
            << std::endl;
}

} /* namespace datasketches */
