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

#include "kll_accuracy_profile.hpp"

namespace datasketches {

void kll_accuracy_profile::run() {
  const unsigned lg_min = 0;
  const unsigned lg_max = 23;
  const unsigned ppo = 16;
  const unsigned num_trials = 100;
  const unsigned error_pct = 99;

  double rank_errors[num_trials];

  unsigned max_len = 1 << lg_max;
  float* values = new float[max_len];

  const unsigned num_steps = count_points(lg_min, lg_max, ppo);
  unsigned stream_length = 1 << lg_min;
  for (unsigned i = 0; i < num_steps; i++) {
    for (unsigned i = 0; i < stream_length; i++) values[i] = i;

    for (unsigned t = 0; t < num_trials; t++) {
      const double maxRankErrorInTrial = run_trial(values, stream_length);
      rank_errors[t] = maxRankErrorInTrial;
    }

    std::sort(&rank_errors[0], &rank_errors[num_trials]);
    const unsigned error_pct_index = num_trials * error_pct / 100;
    const double rank_error = rank_errors[error_pct_index];

    std::cout << stream_length << "\t" << rank_error * 100 << std::endl;

    stream_length = pwr_2_law_next(ppo, stream_length);
  }
  delete [] values;
}

}
