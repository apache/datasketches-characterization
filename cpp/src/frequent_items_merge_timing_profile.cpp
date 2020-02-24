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
#include <random>
#include <chrono>
#include <sstream>

#include <frequent_items_sketch.hpp>

#include "frequent_items_merge_timing_profile.hpp"
#include "zipf_distribution.hpp"

namespace datasketches {

void frequent_items_merge_timing_profile::run() {
  const unsigned lg_min_stream_len = 0;
  const unsigned lg_max_stream_len = 23;
  const unsigned ppo = 16;

  const unsigned lg_max_trials = 14;
  const unsigned lg_min_trials = 4;

  const unsigned lg_max_sketch_size = 10;

  const unsigned zipf_lg_range = 13; // range: 8K values for 1K sketch
  const double zipf_exponent = 1.1;
  zipf_distribution zipf(1 << zipf_lg_range, zipf_exponent);
  const unsigned long long high_bit = 1ULL << 63;

  const size_t num_sketches = 32;

  size_t max_len = 1 << lg_max_stream_len;
  std::vector<std::string> values(max_len);
  std::unique_ptr<frequent_items_sketch<std::string>> sketches[num_sketches];

  std::cout << "StreamLen\tTrials\tBuild\tUpdate\tMerge\tMaxErr\tNumItems" << std::endl;

  size_t stream_length = 1 << lg_min_stream_len;
  while (stream_length <= 1 << lg_max_stream_len) {
    std::chrono::nanoseconds build_time_ns(0);
    std::chrono::nanoseconds update_time_ns(0);
    std::chrono::nanoseconds merge_time_ns(0);
    unsigned num_items = 0;
    unsigned max_error = 0;

    const size_t num_trials = get_num_trials(stream_length, lg_min_stream_len, lg_max_stream_len, lg_min_trials, lg_max_trials);

    for (size_t t = 0; t < num_trials; t++) {
      const auto start_build(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < num_sketches; i++) {
        sketches[i] = std::unique_ptr<frequent_items_sketch<std::string>>(new frequent_items_sketch<std::string>(lg_max_sketch_size));
      }
      frequent_items_sketch<std::string> merge_sketch(lg_max_sketch_size);
      const auto finish_build(std::chrono::high_resolution_clock::now());
      build_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_build - start_build);

      // prepare values to exclude cost from the update loop
      // set the highest bit in 64-bit value to make strings longer so we can compare copying and moving better
      for (size_t i = 0; i < stream_length; i++) values[i] = std::to_string(zipf.sample() | high_bit);

      // spray input evenly across all sketches to be merged
      const auto start_update(std::chrono::high_resolution_clock::now());
      size_t i = 0;
      for (size_t j = 0; j < stream_length; j++) {
        sketches[i]->update(values[j]);
        i++;
        if (i == num_sketches) i = 0;
      }
      const auto finish_update(std::chrono::high_resolution_clock::now());
      update_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_update - start_update);

      const auto start_merge(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < num_sketches; i++) {
        //merge_sketch.merge(*sketches[i]);
        merge_sketch.merge(std::move(*sketches[i]));
      }
      const auto finish_merge(std::chrono::high_resolution_clock::now());
      merge_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_merge - start_merge);

      num_items += merge_sketch.get_num_active_items();
      max_error += merge_sketch.get_maximum_error();
    }

    std::cout << stream_length << "\t"
        << num_trials << "\t"
        << (double) build_time_ns.count() / num_trials << "\t"
        << (double) update_time_ns.count() / num_trials / stream_length << "\t"
        << (double) merge_time_ns.count() / num_trials << "\t"
        << (double) max_error / num_trials << "\t"
        << (double) num_items / num_trials
        << std::endl;

    stream_length = pwr_2_law_next(ppo, stream_length);
  }
}

}
