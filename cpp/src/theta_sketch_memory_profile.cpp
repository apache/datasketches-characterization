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

#include <theta_sketch.hpp>

#include "theta_sketch_memory_profile.hpp"
#include "counting_allocator.hpp"

namespace datasketches {

// this relies on a global variable to count total amount of allocated memory
extern thread_local long long int total_allocated_memory;

void theta_sketch_memory_profile::run_trial(size_t lg_min_x, size_t num_points, size_t x_ppo) {
  const size_t lg_k = 26;
  total_allocated_memory = 0;
  uint64_t key = 0;

  typedef update_theta_sketch_alloc<counting_allocator<uint64_t>> update_theta_sketch_a;
  update_theta_sketch_a::builder builder;
  builder.set_lg_k(lg_k);
  update_theta_sketch_a* s = new (counting_allocator<update_theta_sketch_a>().allocate(1)) update_theta_sketch_a(builder.build());

  size_t count = 0;
  size_t p = 1 << lg_min_x;
  for (size_t i = 0; i < num_points; i++) {
    const size_t delta = p - count;
    for (size_t j = 0; j < delta; j++) {
      s->update(key++);
    }
    count += delta;
    #pragma omp critical(memory_usage_stats_update)
    {
      stats[i].update(total_allocated_memory);
    }
    p = pwr_2_law_next(x_ppo, p);
  }

  s->~update_theta_sketch_a();
  counting_allocator<update_theta_sketch_a>().deallocate(s, 1);
}

}
