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

#include "distinct_count_accuracy_profile.hpp"

namespace datasketches {

accuracy_stats::accuracy_stats(size_t k, size_t true_value):
true_value(true_value),
sum_est(0),
sum_rel_err(0),
sum_sq_rel_err(0),
count(0),
rel_err_distribution(k)
{}

void accuracy_stats::update(double estimate) {
  sum_est += estimate;
  const double relative_error = estimate / true_value - 1.0;
  sum_rel_err += relative_error;
  sum_sq_rel_err += relative_error * relative_error;
  rel_err_distribution.update(relative_error);
  count++;
}

size_t accuracy_stats::get_true_value() const {
  return true_value;
}

double accuracy_stats::get_mean_est() const {
  return sum_est / count;
}

double accuracy_stats::get_mean_rel_err() const {
  return sum_rel_err / count;
}

double accuracy_stats::get_rms_rel_err() const {
  return sqrt(sum_sq_rel_err / count);
}

size_t accuracy_stats::get_count() const {
  return count;
}

std::vector<double> accuracy_stats::get_quantiles(
    const double* fractions, size_t size) const {
  return rel_err_distribution.get_quantiles(fractions, size);
}

/**
 * Manages multiple trials for measuring accuracy.
 *
 * <p>An accuracy trial is run along the distinct count axis (X-axis) first. A single trial
 * consists of a single sketch being updated with the max distinct values, stopping at the
 * configured X-axis points along the way where the accuracy is recorded
 * into the accuracy_stats array. Each instance of accuracy_stats retains the distribution of
 * the relative error and some other measurements for all the trials at that X-axis point.
 *
 * <p>Because accuracy trials take a long time, this profile will output intermediate
 * accuracy results starting after min_trials and then again at trial intervals
 * determined by tppo until max_trials. This allows to stop the testing at
 * any intermediate trials point if sufficient accuracy is achieved.
 */
void distinct_count_accuracy_profile::run() {
  const size_t lg_min_trials = 4;
  const size_t lg_max_trials = 16;
  const size_t trials_ppo = 4;
  const bool print_intermediate = true; // print intermediate data

  const size_t minT = 1 << lg_min_trials;
  const size_t max_trials = 1 << lg_max_trials;

  const size_t lg_min_counts = 0;
  const size_t lg_max_counts = 32;
  const size_t counts_ppo = 16;

  const size_t quantiles_k = 10000;

  const size_t num_points = count_points(lg_min_counts, lg_max_counts, counts_ppo);
  size_t p = 1 << lg_min_counts;
  for (size_t i = 0; i < num_points; i++) {
    stats.push_back(accuracy_stats(quantiles_k, p));
    p = pwr_2_law_next(counts_ppo, p);
  }

  key = 0;

  const auto start_time = std::chrono::system_clock::now();

  // this will generate a table of data up to each intermediate number of trials
  size_t last_trials = 0;
  while (last_trials < max_trials) {
    const size_t next_trials = (last_trials == 0) ? minT : pwr_2_law_next(trials_ppo, last_trials);
    const int delta = next_trials - last_trials;
    for (int i = 0; i < delta; i++) {
      run_trial();
    }
    last_trials = next_trials;

    if (print_intermediate or next_trials == max_trials) {
      print_stats();
    }

    std::cout << "Cum Trials             : " << last_trials << std::endl;
    std::cout << "Cum Updates            : " << key << std::endl;
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

void distinct_count_accuracy_profile::print_stats() const {
  for (const auto& stat: stats) {
    std::cout << stat.get_true_value() << "\t";
    std::cout << stat.get_count() << "\t";
    std::cout << stat.get_mean_est() << "\t";
    std::cout << stat.get_mean_rel_err() << "\t";
    std::cout << stat.get_rms_rel_err() << "\t";
    // quantiles
    const auto quants = stat.get_quantiles(FRACTIONS, FRACT_LEN);
    for (size_t i = 0; i < FRACT_LEN; i++) {
      const double quantile = quants[i];
      std::cout << quantile;
      if (i != FRACT_LEN - 1) std::cout << "\t";
    }
    std::cout << std::endl;
  }
}

}
