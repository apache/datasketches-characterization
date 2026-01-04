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
#include <cmath>
#include <chrono>
#include <ctime>

#include "memory_usage_profile.hpp"
#include "distinct_count_accuracy_profile.hpp"

namespace datasketches {

// global variable for the counting_allocator
thread_local long long int total_allocated_memory;

/**
 * Manages multiple trials for measuring memory usage.
 *
 * <p>Trials are run along the distinct count axis (X-axis) first. A single trial
 * consists of a single sketch being updated with the max number of values, stopping at the
 * configured X-axis points along the way where the memory usage is recorded
 * into the stats array. Each instance of stats retains the distribution of
 * the memory usage for all the trials at that X-axis point.
 *
 * <p>Because trials may take a long time, this profile will output intermediate
 * results starting after min_trials and then again at trial intervals
 * determined by tppo until max_trials. This allows to stop the testing at
 * any intermediate trials point if sufficient number of trials is achieved.
 */
void memory_usage_profile::run() {
  const size_t lg_min_trials = 2;
  const size_t lg_max_trials = 16;
  const size_t trials_ppo = 4;
  const bool print_intermediate = true; // print intermediate data

  const size_t minT = 1 << lg_min_trials;
  const size_t max_trials = 1 << lg_max_trials;

  const size_t lg_min_x = 0;
  const size_t lg_max_x = 32;
  const size_t x_ppo = 16;

  const size_t quantiles_k = 10000;

  const size_t num_points = count_points(lg_min_x, lg_max_x, x_ppo);
  size_t p = 1 << lg_min_x;
  for (size_t i = 0; i < num_points; i++) {
    stats.push_back(kll_sketch<int>(quantiles_k));
    p = pwr_2_law_next(x_ppo, p);
  }

  total_allocated_memory = 0;

  const auto start_time = std::chrono::system_clock::now();

  // this will generate a table of data up to each intermediate number of trials
  size_t last_trials = 0;
  while (last_trials < max_trials) {
    const size_t next_trials = (last_trials == 0) ? minT : pwr_2_law_next(trials_ppo, last_trials);
    const int delta = next_trials - last_trials;
    #pragma omp parallel for schedule(static)
    for (int i = 0; i < delta; i++) {
      run_trial(lg_min_x, num_points, x_ppo);
    }
    last_trials = next_trials;

    if (print_intermediate or next_trials == max_trials) {
      print_stats(lg_min_x, num_points, x_ppo);
    }

    std::cout << "Cum Trials             : " << last_trials << std::endl;
    const auto current_time = std::chrono::system_clock::now();
    const std::chrono::milliseconds cum_time_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(current_time - start_time);
    std::cout << "Cum Time, ms           : " << cum_time_ms.count() << std::endl;
    const double time_per_trial_ms = (cum_time_ms.count()) / last_trials;
    std::cout << "Avg Time Per Trial, ms : " << time_per_trial_ms << std::endl;

    const auto current_time_t = std::chrono::system_clock::to_time_t(current_time);
    std::cout << "Current time           : " << std::ctime(&current_time_t);

    const auto time_to_complete_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        cum_time_ms / last_trials * (max_trials - last_trials));
    const auto est_completion_time = std::chrono::system_clock::to_time_t(current_time + time_to_complete_ms);
    std::cout << "Est Time of Completion : " << std::ctime(&est_completion_time);

    std::cout << std::endl;
  }
}

void memory_usage_profile::print_stats(size_t lg_min_x, size_t num_points, size_t x_ppo) const {
  size_t p = 1 << lg_min_x;
  for (size_t i = 0; i < num_points; i++) {
    std::cout << p << "\t";
    std::cout << stats[i].get_n() << "\t";
    // quantiles
    std::vector<double> quants;
    quants.reserve(FRACT_LEN);
    for (size_t i = 0; i < FRACT_LEN; i++) {
      quants.push_back(stats[i].get_quantile(FRACTIONS[i]));
    }
    for (size_t i = 0; i < FRACT_LEN; i++) {
      const double quantile = quants[i];
      std::cout << quantile;
      if (i != FRACT_LEN - 1) std::cout << "\t";
    }
    std::cout << std::endl;
    p = pwr_2_law_next(x_ppo, p);
  }
}

}
