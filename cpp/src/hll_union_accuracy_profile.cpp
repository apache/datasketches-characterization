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

#include <hll.hpp>

#include "hll_union_accuracy_profile.hpp"

namespace datasketches {

void hll_union_accuracy_profile::run_trial() {
  const size_t lg_k = 12;
  const size_t num_sketches = 32;

  std::unique_ptr<hll_sketch> sketches[num_sketches];
  for (size_t i = 0; i < num_sketches; i++) {
    sketches[i] = std::unique_ptr<hll_sketch>(new hll_sketch(lg_k));
  }
  hll_union u(lg_k);

  size_t count = 0;
  size_t i = 0;
  for (auto& stat: stats) {
    const size_t delta = stat.get_true_value() - count;
    for (size_t j = 0; j < delta; j++) {
      sketches[i++]->update(key++);
      if (i == num_sketches) i = 0;
    }
    count += delta;
    for (auto& sketch: sketches) {
      u.update(std::move(*sketch));
    }
    stat.update(
      u.get_estimate(),
      u.get_lower_bound(1),
      u.get_lower_bound(2),
      u.get_lower_bound(3),
      u.get_upper_bound(1),
      u.get_upper_bound(2),
      u.get_upper_bound(3)
    );
  }
}

}
