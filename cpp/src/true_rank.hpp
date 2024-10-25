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

#ifndef TRUE_RANK_HPP_
#define TRUE_RANK_HPP_

#include <vector>
#include <algorithm>

namespace datasketches {

// assumes sorted values, 0 <= rank <= 1
// only the first n = size values in the vector should be used
template<typename T>
T get_quantile(const std::vector<T>& values, size_t size, double rank) {
  return values[(size - 1) * rank];
}

enum rank_mode {INCLUSIVE, EXCLUSIVE, MIDPOINT};

// assumes sorted values, given value is one of the values in the vector
// only the first n values in the vector should be used
template<typename T>
double get_rank(const std::vector<T>& values, size_t n, T value, rank_mode mode) {
  if (mode == MIDPOINT) {
    auto lower = std::lower_bound(values.begin(), values.begin() + n, value);
    const auto d1 = std::distance(values.begin(), lower);
    auto upper = std::upper_bound(lower, values.begin() + n, value);
    const auto d2 = std::distance(values.begin(), upper);
    return (d1 + d2) / 2.0 / n;
  }
  auto it = mode == INCLUSIVE ? std::upper_bound(values.begin(), values.begin() + n, value)
      : std::lower_bound(values.begin(), values.begin() + n, value);
  const auto d = std::distance(values.begin(), it);
  return static_cast<double>(d) / n;
}

}

#endif
