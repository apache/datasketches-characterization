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

#ifndef REQ_SKETCH_TIMING_PROFILE_IMPL_HPP_
#define REQ_SKETCH_TIMING_PROFILE_IMPL_HPP_

#include <iostream>
#include <algorithm>
#include <random>
#include <chrono>

#include <req_sketch.hpp>

namespace datasketches {

template<typename T>
req_sketch_timing_profile<T>::req_sketch_timing_profile():
generator(std::chrono::system_clock::now().time_since_epoch().count()),
distribution(0.0, 1.0)
{}

template<typename T>
void req_sketch_timing_profile<T>::run() {
  const size_t lg_min_stream_len(0);
  const size_t lg_max_stream_len(23);
  const size_t ppo(16);

  const size_t lg_max_trials(14);
  const size_t lg_min_trials(4);

  const size_t num_queries(20);

  const bool hra = true;
  const uint16_t k = 12;

  std::cout << "Stream\tTrials\tBuild\tUpdate\tQuant\tQuants\tRank\tCDF\tSer\tDeser\tItems\tSize" << std::endl;

  size_t max_len = 1 << lg_max_stream_len;

  std::vector<T> values(max_len);

  std::vector<T> rank_query_values(num_queries);
  for (size_t i = 0; i < num_queries; i++) rank_query_values[i] = sample();
  std::sort(&rank_query_values[0], &rank_query_values[num_queries]);

  double quantile_query_values[num_queries];
  for (size_t i = 0; i < num_queries; i++) quantile_query_values[i] = distribution(generator);

  size_t stream_length(1 << lg_min_stream_len);
  while (stream_length <= (1 << lg_max_stream_len)) {

    std::chrono::nanoseconds build_time_ns(0);
    std::chrono::nanoseconds update_time_ns(0);
    std::chrono::nanoseconds get_quantile_time_ns(0);
    std::chrono::nanoseconds get_quantiles_time_ns(0);
    std::chrono::nanoseconds get_rank_time_ns(0);
    std::chrono::nanoseconds get_cdf_time_ns(0);
    std::chrono::nanoseconds serialize_time_ns(0);
    std::chrono::nanoseconds deserialize_time_ns(0);
    size_t num_retained(0);
    size_t size_bytes(0);

    const size_t num_trials = get_num_trials(stream_length, lg_min_stream_len, lg_max_stream_len, lg_min_trials, lg_max_trials);
    for (size_t i = 0; i < num_trials; i++) {
      for (size_t i = 0; i < stream_length; i++) values[i] = sample();

      auto start_build(std::chrono::high_resolution_clock::now());
      req_sketch<T, hra> sketch(k);
      auto finish_build(std::chrono::high_resolution_clock::now());
      build_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_build - start_build);

      auto start_update(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < stream_length; i++) sketch.update(values[i]);
      auto finish_update(std::chrono::high_resolution_clock::now());
      update_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_update - start_update);

      auto start_get_quantile(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < num_queries; i++) sketch.get_quantile(quantile_query_values[i]);
      auto finish_get_quantile(std::chrono::high_resolution_clock::now());
      get_quantile_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_get_quantile - start_get_quantile);

//      auto start_get_quantiles(std::chrono::high_resolution_clock::now());
//      sketch.get_quantiles(quantile_query_values, num_queries);
//      auto finish_get_quantiles(std::chrono::high_resolution_clock::now());
//      get_quantiles_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_get_quantiles - start_get_quantiles);

      auto start_get_rank(std::chrono::high_resolution_clock::now());
      for (size_t i = 0; i < num_queries; i++) {
        volatile double rank = sketch.get_rank(rank_query_values[i]); // volatile to prevent this from being optimized away
      }
      auto finish_get_rank(std::chrono::high_resolution_clock::now());
      get_rank_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_get_rank - start_get_rank);

//      auto start_get_cdf(std::chrono::high_resolution_clock::now());
//      sketch.get_CDF(rank_query_values.data(), num_queries);
//      auto finish_get_cdf(std::chrono::high_resolution_clock::now());
//      get_cdf_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_get_cdf - start_get_cdf);

      auto start_serialize(std::chrono::high_resolution_clock::now());
      auto bytes = sketch.serialize();
      auto finish_serialize(std::chrono::high_resolution_clock::now());
      serialize_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_serialize - start_serialize);

      auto start_deserialize(std::chrono::high_resolution_clock::now());
      auto deserialized_sketch = req_sketch<T, hra>::deserialize(bytes.data(), bytes.size());
      auto finish_deserialize(std::chrono::high_resolution_clock::now());
      deserialize_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_deserialize - start_deserialize);

      num_retained += sketch.get_num_retained();
      size_bytes += bytes.size();
    }
    std::cout << stream_length << "\t"
        << num_trials << "\t"
        << (double) build_time_ns.count() / num_trials << "\t"
        << (double) update_time_ns.count() / num_trials / stream_length << "\t"
        << (double) get_quantile_time_ns.count() / num_trials / num_queries << "\t"
//        << (double) get_quantiles_time_ns.count() / num_trials / num_queries << "\t"
        << (double) get_rank_time_ns.count() / num_trials / num_queries << "\t"
//        << (double) get_cdf_time_ns.count() / num_trials / num_queries << "\t"
        << (double) serialize_time_ns.count() / num_trials << "\t"
        << (double) deserialize_time_ns.count() / num_trials << "\t"
        << num_retained / num_trials << "\t"
        << size_bytes / num_trials << std::endl;
    stream_length = pwr_2_law_next(ppo, stream_length);
  }
}

template<>
float req_sketch_timing_profile<float>::sample() {
  return distribution(generator);
}

template<>
std::string req_sketch_timing_profile<std::string>::sample() {
  return std::to_string(distribution(generator));
}

}

#endif
