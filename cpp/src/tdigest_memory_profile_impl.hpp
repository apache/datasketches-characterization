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

#ifndef TDIGEST_MEMORY_PROFILE_IMPL_HPP_
#define TDIGEST_MEMORY_PROFILE_IMPL_HPP_

#include <tdigest.hpp>
#include <req_sketch.hpp>

#include "counting_allocator.hpp"

namespace datasketches {

// this relies on a global variable to count total amount of allocated memory
extern thread_local long long int total_allocated_memory;

template<typename T>
void tdigest_memory_profile<T>::run_trial(size_t lg_min_x, size_t num_points, size_t x_ppo) {
  const size_t k = 40;
  total_allocated_memory = 0;

  thread_local std::mt19937 gen(std::random_device{}());
  std::uniform_real_distribution<T> dist(0, 1.0);

//  using tdigest_t = tdigest<T, counting_allocator<T>>;
//  tdigest_t* td = new (counting_allocator<tdigest_t>().allocate(1)) tdigest_t(k);

  using req_sketch_t = req_sketch<T, std::less<T>, counting_allocator<T>>;
  req_sketch_t* s = new (counting_allocator<req_sketch_t>().allocate(1)) req_sketch_t(k);

  size_t count = 0;
  size_t p = 1ULL << lg_min_x;
  for (size_t i = 0; i < num_points; ++i) {
    const size_t delta = p - count;
    for (size_t j = 0; j < delta; ++j) {
      s->update(dist(gen));
    }
    count += delta;
    #pragma omp critical(memory_usage_stats_update)
    {
      stats[i].update(total_allocated_memory);
    }
    p = pwr_2_law_next(x_ppo, p);
  }

//  td->~tdigest_t();
//  counting_allocator<tdigest_t>().deallocate(td, 1);
  s->~req_sketch_t();
  counting_allocator<req_sketch_t>().deallocate(s, 1);
  if (total_allocated_memory != 0) throw std::runtime_error("total_allocated_memory != 0");
}

}

#endif
