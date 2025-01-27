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

#ifndef TDIGEST_TIMING_PROFILE_IMPL_HPP_
#define TDIGEST_TIMING_PROFILE_IMPL_HPP_

#include <iostream>
#include <algorithm>
#include <chrono>
#include <sstream>

#include <tdigest.hpp>

namespace datasketches {

template<typename T>
tdigest_timing_profile<T>::tdigest_timing_profile():
generator(std::chrono::system_clock::now().time_since_epoch().count()),
distribution(0.0, 1.0)
{}

template<typename T>
void tdigest_timing_profile<T>::run() {
  const size_t lg_min_stream_len(0);
  const size_t lg_max_stream_len(23);
  const size_t ppo(16);

  const size_t lg_max_trials(16);
  const size_t lg_min_trials(6);

  const size_t num_queries(20);

  std::cout << "Stream\tTrials\tBuild\tUpdate\tRank" << std::endl;

  std::vector<T> values(1ULL << lg_max_stream_len, 0);

  std::vector<T> rank_query_values(num_queries, 0);
  for (size_t i = 0; i < num_queries; i++) rank_query_values[i] = sample();

  size_t stream_length(1 << lg_min_stream_len);
  while (stream_length <= (1 << lg_max_stream_len)) {

    std::chrono::nanoseconds build_time_ns(0);
    std::chrono::nanoseconds update_time_ns(0);
    std::chrono::nanoseconds get_rank_time_ns(0);
    size_t size_bytes = 0;

    const size_t num_trials = get_num_trials(stream_length, lg_min_stream_len, lg_max_stream_len, lg_min_trials, lg_max_trials);
    for (size_t t = 0; t < num_trials; ++t) {
      std::generate(values.begin(), values.begin() + stream_length, [this] { return this->sample(); });

      auto start_build(std::chrono::high_resolution_clock::now());
      tdigest<T> sketch;
      auto finish_build(std::chrono::high_resolution_clock::now());
      build_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_build - start_build);

      auto start_update(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < stream_length; ++i) sketch.update(values[i]);
      auto finish_update(std::chrono::high_resolution_clock::now());
      update_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_update - start_update);

//      auto start_get_rank(std::chrono::high_resolution_clock::now());
//      for (size_t i = 0; i < num_queries; i++) {
//        volatile double rank = sketch.get_rank(rank_query_values[i]); // volatile to prevent this from being optimized away
//      }
//      auto finish_get_rank(std::chrono::high_resolution_clock::now());
//      get_rank_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_get_rank - start_get_rank);

      size_bytes += sketch.get_serialized_size_bytes();
    }
    std::cout << stream_length << "\t"
        << num_trials << "\t"
        << (double) build_time_ns.count() / num_trials << "\t"
        << (double) update_time_ns.count() / num_trials / stream_length << "\t"
//        << (double) get_rank_time_ns.count() / num_trials / num_queries << "\t"
        << (double) size_bytes / num_trials << "\n";

    stream_length = pwr_2_law_next(ppo, stream_length);
  }
}

template<typename T>
T tdigest_timing_profile<T>::sample() {
  return distribution(generator);
}

}

#endif
