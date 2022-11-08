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

#include <algorithm>
#include <random>
#include <chrono>

#include <kll_sketch.hpp>

#include "kll_sketch_accuracy_profile.hpp"

namespace datasketches {

double kll_sketch_accuracy_profile::run_trial(float* values, unsigned stream_length) {
  unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::shuffle(values, values + stream_length, std::default_random_engine(seed));

  kll_sketch<float> sketch;
  for (size_t i = 0; i < stream_length; i++) sketch.update(values[i]);

  double max_rank_error = 0;
  for (size_t i = 1; i <= stream_length; i++) {
    double true_rank = static_cast<double>(i) / stream_length;
    double est_rank = sketch.get_rank(i);
    max_rank_error = std::max(max_rank_error, fabs(true_rank - est_rank));
  }

  return max_rank_error;
}

}
