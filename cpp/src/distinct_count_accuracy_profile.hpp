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

#ifndef DISTINCT_COUNT_ACCURACY_PROFILE_HPP_
#define DISTINCT_COUNT_ACCURACY_PROFILE_HPP_

#include "job_profile.hpp"
#include "stddev.hpp"
#include "kll_sketch.hpp"

namespace datasketches {

// quantile fractions computed from the standard normal cumulative distribution.
// static const double M3SD = 0.0013498980316301; //minus 3 StdDev
// static const double M2SD = 0.0227501319481792; //minus 2 StdDev
// static const double M1SD = 0.1586552539314570; //minus 1 StdDev
// static const double P1SD = 0.8413447460685430; //plus  1 StdDev
// static const double P2SD = 0.9772498680518210; //plus  2 StdDev
// static const double P3SD = 0.9986501019683700; //plus  3 StdDev
static const double FRACTIONS[] = {0.0, M3SD, M2SD, M1SD, 0.5, P1SD, P2SD, P3SD, 1.0};
static const size_t FRACT_LEN = 9;

class accuracy_stats {
public:
  accuracy_stats(size_t k, size_t true_value);
  void update(double estimate);
  size_t get_true_value() const;
  double get_mean_est() const;
  double get_mean_rel_err() const;
  double get_rms_rel_err() const;
  size_t get_count() const;
  std::vector<double> get_quantiles(const double* fractions, size_t size) const;

private:
  size_t true_value;
  double sum_est;
  double sum_rel_err;
  double sum_sq_rel_err;
  size_t count;
  kll_sketch<double> rel_err_distribution;
};

class distinct_count_accuracy_profile: public job_profile {
public:
  void run();
  virtual void run_trial() = 0;

protected:
  uint64_t key;
  std::vector<accuracy_stats> stats;

private:
  void print_stats() const;
};

}

#endif
