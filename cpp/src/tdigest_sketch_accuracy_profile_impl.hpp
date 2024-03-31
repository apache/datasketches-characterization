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

#ifndef TDIGEST_SKETCH_ACCURACY_PROFILE_IMPL_HPP_
#define TDIGEST_SKETCH_ACCURACY_PROFILE_IMPL_HPP_

#include <tdigest.hpp>

namespace datasketches {

// assumes sorted values, 0 <= rank <= 1
// only the first n = size values in the vector should be used
template<typename T>
T get_quantile(const std::vector<T>& values, size_t size, double rank) {
  return values[(size - 1) * rank];
}

// assumes sorted values, given value is one of the values in the vector
// only the first n = size values in the vector should be used
template<typename T>
double get_rank(const std::vector<T>& values, size_t size, T value) {
  auto lower = std::lower_bound(values.begin(), values.begin() + size, value);
  const auto d1 = std::distance(values.begin(), lower);
  auto upper = std::upper_bound(lower, values.begin() + size, value);
  const auto d2 = std::distance(values.begin(), upper);
  return (d1 + d2) / 2.0 / size;
}

template<typename T>
void tdigest_sketch_accuracy_profile<T>::run_trial(std::vector<T>& values, size_t stream_length, uint16_t k,
    const std::vector<double>& ranks, std::vector<std::vector<double>>& rank_errors) {

  tdigest<T> sketch(k);
  for (size_t i = 0; i < stream_length; ++i) sketch.update(values[i]);

  std::sort(values.begin(), values.begin() + stream_length);
  unsigned j = 0;
  for (const double rank: ranks) {
    const T quantile = get_quantile(values, stream_length, rank);
    const double true_rank = get_rank(values, stream_length, quantile);
    rank_errors[j++].push_back(std::abs(sketch.get_rank(quantile) - true_rank));
  }
}

}

#endif
