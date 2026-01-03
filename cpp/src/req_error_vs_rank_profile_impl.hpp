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

#ifndef REQ_ERROR_VS_RANK_PROFILE_IMPL_HPP_
#define REQ_ERROR_VS_RANK_PROFILE_IMPL_HPP_

#include <iostream>
#include <algorithm>

#include <req_sketch.hpp>
#include <kll_sketch.hpp>

#include "true_rank.hpp"
#include "stddev.hpp"

namespace datasketches {

template<typename T>
req_error_vs_rank_profile<T>::req_error_vs_rank_profile():
generator(std::chrono::system_clock::now().time_since_epoch().count()),
distribution(0.0, 1.0)
{}

template<typename T>
void req_error_vs_rank_profile<T>::run() {
  const size_t lg_stream_len = 25;
  const size_t plot_points = 100;
  const size_t num_trials = 10000;

  // req sketch parameters
  const bool hra = true;
  const uint16_t k = 12;

  const uint16_t error_sketch_k = 1000;
  size_t stream_len = 1 << lg_stream_len;

  std::vector<double> plot_ranks(plot_points);
  for (size_t i = 0; i < plot_points; ++i) {
    plot_ranks[i] = static_cast<double>(i) / (plot_points - 1);
  }

  // Global result (merged after)
  std::vector<kll_sketch<double>> error_distributions(plot_points, kll_sketch<double>(error_sketch_k));

  std::cout << "Trials: " << num_trials << "\n";
  std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
  #pragma omp parallel
  {
    std::vector<kll_sketch<double>> local_error_distributions(plot_points, kll_sketch<double>(error_sketch_k));
    std::vector<T> values(stream_len);

    #pragma omp for schedule(static)
    for (size_t t = 1; t <= num_trials; ++t) {
      std::generate(values.begin(), values.end(), [this]{return sample();});
      //    req_sketch<T> sketch(k, hra);
      tdigest<T> sketch(100);
      for (auto value: values) {
        sketch.update(value);
      }
      std::sort(values.begin(), values.end());
      for (size_t i = 0; i < plot_points; ++i) {
        const T quantile = get_quantile(values, values.size(), plot_ranks[i]);
        //      const double true_rank = get_rank(values, values.size(), quantile, INCLUSIVE);
        const double true_rank = get_rank(values, values.size(), quantile, MIDPOINT);
        local_error_distributions[i].update(sketch.get_rank(quantile) - true_rank);
      }
    }

    // Merge local sketches into global one
    #pragma omp critical
    {
      for (size_t i = 0; i < plot_points; ++i) {
        error_distributions[i].merge(local_error_distributions[i]);
      }
    }
  }
  std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
  std::cout << "Duration: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << " ms" << std::endl;

  std::cout << "Rank\t-3SD\t-2SD\t-1SD\tMed\t+1SD\t+2SD\t+3SD\n";
  for (size_t i = 0; i < plot_points; ++i) {
    std::cout << plot_ranks[i] << "\t";
    std::cout << error_distributions[i].get_quantile(M3SD) << "\t";
    std::cout << error_distributions[i].get_quantile(M2SD) << "\t";
    std::cout << error_distributions[i].get_quantile(M1SD) << "\t";
    std::cout << error_distributions[i].get_quantile(0.5) << "\t";
    std::cout << error_distributions[i].get_quantile(P1SD) << "\t";
    std::cout << error_distributions[i].get_quantile(P2SD) << "\t";
    std::cout << error_distributions[i].get_quantile(P3SD) << "\n";
  }
}

template<typename T>
T req_error_vs_rank_profile<T>::sample() {
  return distribution(generator);
}

}

#endif
