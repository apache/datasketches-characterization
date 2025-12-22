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

#ifndef TDIGEST_ACCURACY_PROFILE_IMPL_HPP_
#define TDIGEST_ACCURACY_PROFILE_IMPL_HPP_

#include <iostream>
#include <algorithm>
#include <cmath>
#include <random>
#include <chrono>

#include "tdigest_accuracy_profile.hpp"

#include <omp.h>

namespace datasketches {

template<typename T>
void tdigest_accuracy_profile<T>::run() {
  const unsigned lg_min = 0;
  const unsigned lg_max = 23;
  const unsigned ppo = 8;
  const unsigned num_trials = 1000;
  const unsigned error_pct = 99;

  const uint16_t compression = 200;
  const std::vector<double> ranks = {0.01, 0.05, 0.5, 0.95, 0.99};

  std::cout << "N";
  for (const double rank: ranks) std::cout << "\terr at " << rank;
  std::cout << "\n";

  const unsigned num_steps = count_points(lg_min, lg_max, ppo);
  std::vector<std::vector<double>> rank_errors(ranks.size(), std::vector<double>(num_trials, 0));
  unsigned stream_length = 1;

  for (unsigned i = 0; i < num_steps; ++i) {
    std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    #pragma omp parallel for
    for (unsigned t = 0; t < num_trials; t++) {
      std::random_device rd;
      std::mt19937 gen( rd());
      //  std::uniform_real_distribution<T> dist(0, 1.0);
      std::exponential_distribution<T> dist(1.5);

      std::vector<T> values;
      values.resize(stream_length);
      for (size_t j = 0; j < stream_length; ++j) {
        values[j] = dist(gen);
      }
      run_trial(values, stream_length, compression, ranks, rank_errors, t);
    }
    std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;

    std::cout << stream_length;

    for (auto& errors: rank_errors) {
      std::sort(errors.begin(), errors.end());
      const size_t error_pct_index = num_trials * error_pct / 100;
      const double rank_error = errors[error_pct_index];
      std::cout << "\t" << rank_error * 100;
    }
    std::cout << "\n";

    stream_length = pwr_2_law_next(ppo, stream_length);
  }
}

}

#endif
