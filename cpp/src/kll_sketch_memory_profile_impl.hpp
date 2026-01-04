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

#ifndef KLL_SKETCH_MEMORY_PROFILE_IMPL_HPP_
#define KLL_SKETCH_MEMORY_PROFILE_IMPL_HPP_

#include <kll_sketch.hpp>

#include "counting_allocator.hpp"

namespace datasketches {

// this relies on a global variable to count total amount of allocated memory
extern thread_local long long int total_allocated_memory;

template<typename T>
void kll_sketch_memory_profile<T>::run_trial(size_t lg_min_x, size_t num_points, size_t x_ppo) {
  const size_t k = 200;
  total_allocated_memory = 0;
  T value = 0;

  typedef kll_sketch<T, std::less<T>, counting_allocator<T>> kll_sketch_t;
  kll_sketch_t* s = new (counting_allocator<kll_sketch_t>().allocate(1)) kll_sketch_t(k);

  size_t count = 0;
  size_t p = 1 << lg_min_x;
  for (size_t i = 0; i < num_points; i++) {
    const size_t delta = p - count;
    for (size_t j = 0; j < delta; j++) {
      s->update(value);
      value += 1; // distribution does not really matter
    }
    count += delta;
    #pragma omp critical(memory_usage_stats_update)
    {
      stats[i].update(total_allocated_memory);
    }
    p = pwr_2_law_next(x_ppo, p);
  }

  s->~kll_sketch_t();
  counting_allocator<kll_sketch_t>().deallocate(s, 1);
}

}

#endif
