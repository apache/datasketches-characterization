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

#include <frequent_items_sketch.hpp>

#include "frequent_items_sketch_accuracy_profile.hpp"
#include "zipf_distribution.hpp"

namespace datasketches {

void frequent_items_sketch_accuracy_profile::run() {
  const unsigned lg_num_sketches = 4; // merge if > 0 (more than 1 sketch)

  const unsigned lg_min_stream_len = 5;
  const unsigned lg_max_stream_len = 23;
  const unsigned ppo = (lg_num_sketches > 0) ? 1 : 16; // simple partitioning for merging relies on powers of 2

  const unsigned lg_max_trials = 18;
  const unsigned lg_min_trials = 10;

  const unsigned lg_max_sketch_size = 10;

  const unsigned zipf_lg_range = 13; // range: 8K values for 1K sketch
  const double zipf_exponent = 0.7;

  zipf_distribution zipf(1 << zipf_lg_range, zipf_exponent);

  size_t stream_length = 1 << lg_min_stream_len;
  while (stream_length <= 1 << lg_max_stream_len) {
    unsigned num_items = 0;
    unsigned max_error = 0;
    unsigned num_error_1 = 0;
    unsigned num_error_2 = 0;
    unsigned extra_items = 0;
    unsigned num_error_3 = 0;

    const size_t num_trials = get_num_trials(stream_length, lg_min_stream_len, lg_max_stream_len, lg_min_trials, lg_max_trials);

    // trust sketch to compute epsilon
    unsigned threshold = frequent_items_sketch<unsigned>::get_epsilon(lg_max_sketch_size) * stream_length;

    unsigned* values = new unsigned[stream_length];

    for (size_t i = 0; i < num_trials; i++) {
      // prepare values for this trial
      for (size_t j = 0; j < stream_length; j++) {
        values[j] = zipf.sample();
      }

      frequent_items_sketch<unsigned> sketch(lg_max_sketch_size);
      if (lg_num_sketches == 0) {
        for (size_t j = 0; j < stream_length; j++) {
          sketch.update(values[j]);
        }
      } else {
        // this partitioning relies on stream length being power of 2, so ppo must be 1
        const unsigned substream_length = stream_length / (1 << lg_num_sketches);
        for (size_t j = 0; j < (1 << lg_num_sketches); j++) {
          frequent_items_sketch<unsigned> s(lg_max_sketch_size);
          for (size_t k = 0; k < substream_length; k++) {
            s.update(values[j * substream_length + k]);
          }
          sketch.merge(s);
        }
      }
      num_items += sketch.get_num_active_items();
      max_error += sketch.get_maximum_error();

      // brute-force frequent items
      std::unordered_map<unsigned, unsigned> frequencies;
      for (size_t j = 0; j < stream_length; j++) {
        frequencies[values[j]]++;
      }
      std::unordered_map<unsigned, unsigned> frequent_items;
      for (auto it: frequencies) {
        if (it.second > threshold) frequent_items[it.first] = it.second;
      }

      // checks

      // using a priori threshold (conservative)

      // this must be a subset of the exact solution (only frequent items above threshold, not necessarily all of them)
      auto conservative_no_false_positives = sketch.get_frequent_items(frequent_items_error_type::NO_FALSE_POSITIVES, threshold);
      for (auto& it: conservative_no_false_positives) {
        if (frequent_items.find(it.get_item()) == frequent_items.end()) {
          num_error_1++;
        }
      }

      // the exact solution must be a subset of this (all frequent items above threshold must be present, and possibly some extra items)
      auto conservative_no_false_negatives = sketch.get_frequent_items(frequent_items_error_type::NO_FALSE_NEGATIVES, threshold);
      std::unordered_map<unsigned, unsigned> conservative_no_false_negatives_map;
      for (auto& it: conservative_no_false_negatives) conservative_no_false_negatives_map[it.get_item()] = it.get_estimate();
      for (auto& it: frequent_items) {
        if (conservative_no_false_negatives_map.find(it.first) == conservative_no_false_negatives_map.end()) {
          num_error_2++;
        }
      }

      // using actual max error as threshold (the default)
      // this is expected to find more items compared to using conservative threshold

      auto no_false_positives = sketch.get_frequent_items(frequent_items_error_type::NO_FALSE_POSITIVES);
      for (auto& it: no_false_positives) {
        unsigned item = it.get_item();
        if (frequent_items.find(item) == frequent_items.end()) {
          extra_items++;
        }
      }

      // all items of the exact solution must be in this set
      auto no_false_negatives = sketch.get_frequent_items(frequent_items_error_type::NO_FALSE_NEGATIVES);
      std::unordered_map<unsigned, unsigned> no_false_negatives_map;
      for (auto& it: no_false_negatives) no_false_negatives_map[it.get_item()] = it.get_estimate();
      for (auto& it: frequent_items) {
        if (no_false_negatives_map.find(it.first) == no_false_negatives_map.end()) {
          num_error_3++;
        }
      }
    }
    delete [] values;

    std::cout << stream_length
        << "\t" << num_trials
        << "\t" << (double) num_items / num_trials
        << "\t" << threshold
        << "\t" << (double) max_error / num_trials
        << "\t" << (double) num_error_1 / num_trials
        << "\t" << (double) num_error_2 / num_trials
        << "\t" << (double) extra_items / num_trials
        << "\t" << (double) num_error_3 / num_trials
        << std::endl;

    stream_length = pwr_2_law_next(ppo, stream_length);
  }
}

} /* namespace datasketches */
