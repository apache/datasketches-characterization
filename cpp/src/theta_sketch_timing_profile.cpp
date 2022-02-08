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

#include <theta_sketch.hpp>

#include "theta_sketch_timing_profile.hpp"

namespace datasketches {

void theta_sketch_timing_profile::run() {
  const size_t lg_min_stream_len(0);
  const size_t lg_max_stream_len(23);
  const size_t ppo(16);

  const size_t lg_max_trials(18);
  const size_t lg_min_trials(10);

  const uint8_t lg_k = 12;
  update_theta_sketch::builder builder;
  builder.set_lg_k(lg_k);

  // some arbitrary starting value
  uint64_t counter(35538947);

  const uint64_t golden64(0x9e3779b97f4a7c13ULL);  // the golden ratio

  std::cout << "Stream\tTrials\tBuild\tUpdate\tSer\tDeser\tSize" << std::endl;

  size_t stream_length(1 << lg_min_stream_len);
  while (stream_length <= (1 << lg_max_stream_len)) {

    std::chrono::nanoseconds build_time_ns(0);
    std::chrono::nanoseconds update_time_ns(0);
    std::chrono::nanoseconds compact_time_ns(0);
    std::chrono::nanoseconds serialize_time_ns(0);
    std::chrono::nanoseconds deserialize_time_ns(0);
    size_t size_bytes(0);

    const size_t num_trials = get_num_trials(stream_length, lg_min_stream_len, lg_max_stream_len, lg_min_trials, lg_max_trials);

    for (size_t i = 0; i < num_trials; i++) {
      const auto start_build(std::chrono::high_resolution_clock::now());
      auto sketch = builder.build();
      const auto finish_build(std::chrono::high_resolution_clock::now());
      build_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_build - start_build);

      const auto start_update(std::chrono::high_resolution_clock::now());
      for (size_t j = 0; j < stream_length; j++) {
        sketch.update(counter);
        counter += golden64;
      }
      const auto finish_update(std::chrono::high_resolution_clock::now());
      update_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_update - start_update);

      auto start_compacting(std::chrono::high_resolution_clock::now());
      auto compact_sketch = sketch.compact();
      const auto finish_compacting(std::chrono::high_resolution_clock::now());
      compact_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_compacting - start_compacting);

      auto start_serialize(std::chrono::high_resolution_clock::now());
      auto bytes = compact_sketch.serialize();
      const auto finish_serialize(std::chrono::high_resolution_clock::now());
      serialize_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_serialize - start_serialize);

      const auto start_deserialize(std::chrono::high_resolution_clock::now());
      auto deserialized_sketch = compact_theta_sketch::deserialize(bytes.data(), bytes.size());
      const auto finish_deserialize(std::chrono::high_resolution_clock::now());
      deserialize_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_deserialize - start_deserialize);

      size_bytes += bytes.size();
    }

    std::cout << stream_length << "\t"
        << num_trials << "\t"
        << (double) build_time_ns.count() / num_trials << "\t"
        << (double) update_time_ns.count() / num_trials / stream_length << "\t"
        << (double) compact_time_ns.count() / num_trials << "\t"
        << (double) serialize_time_ns.count() / num_trials << "\t"
        << (double) deserialize_time_ns.count() / num_trials << "\t"
        << (double) size_bytes / num_trials << "\t"
        << std::endl;
    stream_length = pwr_2_law_next(ppo, stream_length);
  }

}

}
