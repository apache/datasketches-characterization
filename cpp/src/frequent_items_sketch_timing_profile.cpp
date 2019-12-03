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
#include <random>
#include <chrono>
#include <sstream>

#include <frequent_items_sketch.hpp>

#include "frequent_items_sketch_timing_profile.hpp"
#include "zipf_distribution.hpp"

namespace datasketches {

// This hash function is taken from the internals of Austin Appleby's MurmurHash3 algorithm
struct hash_long_long {
  size_t operator()(long long key) const {
    key ^= key >> 33;
    key *= 0xff51afd7ed558ccdL;
    key ^= key >> 33;
    key *= 0xc4ceb9fe1a85ec53L;
    key ^= key >> 33;
    return key;
  }
};
typedef frequent_items_sketch<long long, hash_long_long> frequent_longs_sketch;

void frequent_items_sketch_timing_profile::run() {
  const unsigned lg_min_stream_len = 0;
  const unsigned lg_max_stream_len = 23;
  const unsigned ppo = 16;

  const unsigned lg_max_trials = 14;
  const unsigned lg_min_trials = 8;

  const unsigned lg_max_sketch_size = 10;

  const unsigned zipf_lg_range = 13; // range: 8K values for 1K sketch
  const double zipf_exponent = 0.7;
  const double geom_p = 0.005;

  std::default_random_engine generator(std::chrono::system_clock::now().time_since_epoch().count());
  std::geometric_distribution<long long> geometric_distribution(geom_p);

  zipf_distribution zipf(1 << zipf_lg_range, zipf_exponent);

  std::cout << "StreamLen\tTrials\tBuild\tUpdate\tSerStream\tDeserStream\tSerBytes\tDeserBytes\tMaxErr\tNumItems\tSizeBytes" << std::endl;

  size_t stream_length = 1 << lg_min_stream_len;
  while (stream_length <= 1 << lg_max_stream_len) {
    std::chrono::nanoseconds build_time_ns(0);
    std::chrono::nanoseconds update_time_ns(0);
    std::chrono::nanoseconds stream_serialize_time_ns(0);
    std::chrono::nanoseconds stream_deserialize_time_ns(0);
    std::chrono::nanoseconds bytes_serialize_time_ns(0);
    std::chrono::nanoseconds bytes_deserialize_time_ns(0);
    unsigned num_items = 0;
    size_t size_bytes = 0;
    size_t max_error = 0;

    const size_t num_trials = get_num_trials(stream_length, lg_min_stream_len, lg_max_stream_len, lg_min_trials, lg_max_trials);

    long long* values = new long long[stream_length];
    for (size_t i = 0; i < num_trials; i++) {
      const auto start_build(std::chrono::high_resolution_clock::now());
      frequent_longs_sketch sketch(lg_max_sketch_size);
      const auto finish_build(std::chrono::high_resolution_clock::now());
      build_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_build - start_build);

      // prepare values to exclude cost of random generator from the update loop
      for (size_t j = 0; j < stream_length; j++) {
        values[j] = zipf.sample();
      }

      const auto start_update(std::chrono::high_resolution_clock::now());
      for (size_t j = 0; j < stream_length; ++j) {
        sketch.update(values[j]);
      }
      const auto finish_update(std::chrono::high_resolution_clock::now());
      update_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_update - start_update);

      {
        std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
        auto start_stream_serialize(std::chrono::high_resolution_clock::now());
        sketch.serialize(s);
        const auto finish_stream_serialize(std::chrono::high_resolution_clock::now());
        stream_serialize_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_stream_serialize - start_stream_serialize);

        const auto start_stream_deserialize(std::chrono::high_resolution_clock::now());
        auto deserialized_sketch = frequent_longs_sketch::deserialize(s);
        const auto finish_stream_deserialize(std::chrono::high_resolution_clock::now());
        stream_deserialize_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_stream_deserialize - start_stream_deserialize);

        size_bytes += s.tellp();
      }

      {
        auto start_bytes_serialize(std::chrono::high_resolution_clock::now());
        auto pair = sketch.serialize();
        const auto finish_bytes_serialize(std::chrono::high_resolution_clock::now());
        bytes_serialize_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_bytes_serialize - start_bytes_serialize);

        const auto start_bytes_deserialize(std::chrono::high_resolution_clock::now());
        auto deserialized_sketch = frequent_longs_sketch::deserialize(pair.first.get(), pair.second);
        const auto finish_bytes_deserialize(std::chrono::high_resolution_clock::now());
        bytes_deserialize_time_ns += std::chrono::duration_cast<std::chrono::nanoseconds>(finish_bytes_deserialize - start_bytes_deserialize);
      }

      num_items += sketch.get_num_active_items();
      max_error += sketch.get_maximum_error();
    }
    delete [] values;

    std::cout << stream_length << "\t"
        << num_trials << "\t"
        << (double) build_time_ns.count() / num_trials << "\t"
        << (double) update_time_ns.count() / num_trials / stream_length << "\t"
        << (double) stream_serialize_time_ns.count() / num_trials << "\t"
        << (double) stream_deserialize_time_ns.count() / num_trials << "\t"
        << (double) bytes_serialize_time_ns.count() / num_trials << "\t"
        << (double) bytes_deserialize_time_ns.count() / num_trials << "\t"
        << (double) max_error / num_trials << "\t"
        << (double) num_items / num_trials << "\t"
        << (double) size_bytes / num_trials << "\t"
        << std::endl;

    stream_length = pwr_2_law_next(ppo, stream_length);
  }
}

}
