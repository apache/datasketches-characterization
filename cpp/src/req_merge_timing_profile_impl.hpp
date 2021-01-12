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

#ifndef REQ_MERGE_TIMING_PROFILE_IMPL_HPP_
#define REQ_MERGE_TIMING_PROFILE_IMPL_HPP_

#include <iostream>
#include <algorithm>
#include <chrono>
#include <sstream>

#include <req_sketch.hpp>

namespace datasketches {

template<typename T>
req_merge_timing_profile<T>::req_merge_timing_profile():
generator(std::chrono::system_clock::now().time_since_epoch().count()),
distribution(0.0, 1.0)
{}

template<typename T>
void req_merge_timing_profile<T>::run() {
  const size_t lg_min_stream_len = 0;
  const size_t lg_max_stream_len = 26;
  const size_t ppo = 16;

  const size_t lg_max_trials = 16;
  const size_t lg_min_trials = 4;

  const bool hra = true;
  const uint16_t k = 12;
  const size_t num_sketches = 32;

  std::cout << "Stream\tTrials\tBuild\tUpdate\tMerge\tItems" << std::endl;

  size_t max_len = 1 << lg_max_stream_len;

  std::vector<T> values(max_len);
  std::unique_ptr<req_sketch<T>> sketches[num_sketches];

  size_t stream_length(1 << lg_min_stream_len);
  while (stream_length <= (1 << lg_max_stream_len)) {
    std::chrono::nanoseconds build_time_ns(0);
    std::chrono::nanoseconds update_time_ns(0);
    std::chrono::nanoseconds merge_time_ns(0);
    size_t num_retained(0);

    const size_t num_trials = get_num_trials(stream_length, lg_min_stream_len, lg_max_stream_len, lg_min_trials, lg_max_trials);
    for (size_t t = 0; t < num_trials; t++) {
      for (size_t i = 0; i < stream_length; i++) values[i] = sample();

      auto start_build(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < num_sketches; i++) {
        sketches[i] = std::unique_ptr<req_sketch<T>>(new req_sketch<T>(k, hra));
      }
      req_sketch<T> merge_sketch(k, hra);
      auto finish_build(std::chrono::high_resolution_clock::now());
      build_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_build - start_build);

      auto start_update(std::chrono::high_resolution_clock::now());
      size_t i = 0;
      for (size_t j = 0; j < stream_length; j++) {
        sketches[i]->update(values[j]);
        i++;
        if (i == num_sketches) i = 0;
      }
      auto finish_update(std::chrono::high_resolution_clock::now());
      update_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_update - start_update);

      const auto start_merge(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < num_sketches; i++) {
        merge_sketch.merge(std::move(*sketches[i]));
      }
      const auto finish_merge(std::chrono::high_resolution_clock::now());
      merge_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_merge - start_merge);

      num_retained += merge_sketch.get_num_retained();
    }
    std::cout << stream_length << "\t"
        << num_trials << "\t"
        << (double) build_time_ns.count() / num_trials << "\t"
        << (double) update_time_ns.count() / num_trials / stream_length << "\t"
        << (double) merge_time_ns.count() / num_trials << "\t"
        << num_retained / num_trials
        << std::endl;
    stream_length = pwr_2_law_next(ppo, stream_length);
  }
}

template<>
float req_merge_timing_profile<float>::sample() {
  return distribution(generator);
}

template<>
std::string req_merge_timing_profile<std::string>::sample() {
  return std::to_string(distribution(generator));
}

}

#endif
