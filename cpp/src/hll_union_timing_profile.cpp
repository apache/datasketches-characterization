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

#include <hll.hpp>

#include "hll_union_timing_profile.hpp"

namespace datasketches {

void hll_union_timing_profile::run() {
  const size_t lg_min_stream_len = 0;
  const size_t lg_max_stream_len = 26;
  const size_t ppo = 16;

  const size_t lg_max_trials = 14;
  const size_t lg_min_trials = 6;

  const int lg_k = 11;
  const int num_sketches_to_union = 32;

  // some arbitrary starting value
  uint64_t counter = 35538947;
  const uint64_t golden64 = 0x9e3779b97f4a7c13ULL;  // the golden ratio

  std::cout << "Stream\tTrials\tBuild\tUpdate\tSerialize\tDeserialize\tUnion\tResult" << std::endl;

  std::unique_ptr<hll_sketch> sketches[num_sketches_to_union];

  size_t stream_length = 1 << lg_min_stream_len;
  while (stream_length <= (1 << lg_max_stream_len)) {

    std::chrono::nanoseconds build_time_ns(0);
    std::chrono::nanoseconds update_time_ns(0);
    std::chrono::nanoseconds serialize_time_ns(0);
    std::chrono::nanoseconds deserialize_time_ns(0);
    std::chrono::nanoseconds union_time_ns(0);
    std::chrono::nanoseconds result_time_ns(0);

    const size_t num_trials = get_num_trials(stream_length, lg_min_stream_len, lg_max_stream_len, lg_min_trials, lg_max_trials);
    for (size_t t = 0; t < num_trials; t++) {
      const auto start_build(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < num_sketches_to_union; i++) {
        sketches[i] = std::unique_ptr<hll_sketch>(new hll_sketch(lg_k));
      }
      hll_union u(lg_k);
      const auto finish_build(std::chrono::high_resolution_clock::now());
      build_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_build - start_build);

      const auto start_update(std::chrono::high_resolution_clock::now());
      size_t i = 0;
      for (size_t j = 0; j < stream_length; j++) {
        sketches[i]->update(counter);
        counter += golden64;
        i++;
        if (i == num_sketches_to_union) i = 0;
      }
      const auto finish_update(std::chrono::high_resolution_clock::now());
      update_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_update - start_update);

      std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
      const auto start_serialize(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < num_sketches_to_union; i++) {
        sketches[i]->serialize_compact(s);
      }
      const auto finish_serialize(std::chrono::high_resolution_clock::now());
      serialize_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_serialize - start_serialize);

      const auto start_deserialize(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < num_sketches_to_union; i++) {
        sketches[i] = std::unique_ptr<hll_sketch>(new hll_sketch(hll_sketch::deserialize(s)));
      }
      const auto finish_deserialize(std::chrono::high_resolution_clock::now());
      deserialize_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_deserialize - start_deserialize);

      const auto start_union(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < num_sketches_to_union; i++) {
        u.update(*sketches[i]);
      }
      const auto finish_union(std::chrono::high_resolution_clock::now());
      union_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_union - start_union);

      const auto start_result(std::chrono::high_resolution_clock::now());
      hll_sketch result = u.get_result();
      const auto finish_result(std::chrono::high_resolution_clock::now());
      result_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_result - start_result);
    }

    std::cout << stream_length << "\t"
        << num_trials << "\t"
        << (double) build_time_ns.count() / num_trials << "\t"
        << (double) update_time_ns.count() / num_trials / stream_length << "\t"
        << (double) serialize_time_ns.count() / num_trials << "\t"
        << (double) deserialize_time_ns.count() / num_trials << "\t"
        << (double) union_time_ns.count() / num_trials << "\t"
        << (double) result_time_ns.count() / num_trials << "\t"
        << std::endl;
    stream_length = pwr_2_law_next(ppo, stream_length);
  }

}

}
