# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

add_executable(characterization)

find_package(DataSketches 5.3 REQUIRED)
target_link_libraries(characterization PUBLIC ${DATASKETCHES_LIB})
target_include_directories(characterization PUBLIC ${DATASKETCHES_INCLUDE_DIR})

find_package(OpenMP REQUIRED)

target_compile_options(characterization PRIVATE
  -O3
  -DNDEBUG
)
target_link_libraries(characterization PRIVATE OpenMP::OpenMP_CXX)

target_compile_features(common INTERFACE cxx_std_11)

set_target_properties(characterization PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

target_sources(characterization
  PRIVATE
    src/main.cpp
    src/job_profile.cpp
    src/job_profile.hpp
    src/counting_allocator.hpp
    src/cpc_sketch_accuracy_profile.cpp
    src/cpc_sketch_accuracy_profile.hpp
    src/cpc_sketch_memory_profile.cpp
    src/cpc_sketch_memory_profile.hpp
    src/cpc_sketch_timing_profile.cpp
    src/cpc_sketch_timing_profile.hpp
    src/cpc_union_accuracy_profile.cpp
    src/cpc_union_accuracy_profile.hpp
    src/cpc_union_timing_profile.cpp
    src/cpc_union_timing_profile.hpp
    src/distinct_count_accuracy_profile.cpp
    src/distinct_count_accuracy_profile.hpp
    src/frequent_items_merge_timing_profile.cpp
    src/frequent_items_merge_timing_profile.hpp
    src/frequent_items_sketch_accuracy_profile.cpp
    src/frequent_items_sketch_accuracy_profile.hpp
    src/frequent_items_sketch_timing_profile.cpp
    src/frequent_items_sketch_timing_profile.hpp
    src/hll_cross_language_profile.cpp
    src/hll_cross_language_profile.hpp
    src/hll_sketch_accuracy_profile.cpp
    src/hll_sketch_accuracy_profile.hpp
    src/hll_sketch_memory_profile.cpp
    src/hll_sketch_memory_profile.hpp
    src/hll_sketch_timing_profile.cpp
    src/hll_sketch_timing_profile.hpp
    src/hll_union_accuracy_profile.cpp
    src/hll_union_accuracy_profile.hpp
    src/hll_union_timing_profile.cpp
    src/hll_union_timing_profile.hpp
    src/kll_accuracy_profile.cpp
    src/kll_accuracy_profile.hpp
    src/kll_merge_accuracy_profile.cpp
    src/kll_merge_accuracy_profile.hpp
    src/kll_merge_timing_profile.hpp
    src/kll_merge_timing_profile_impl.hpp
    src/kll_sketch_accuracy_profile.cpp
    src/kll_sketch_accuracy_profile.hpp
    src/kll_sketch_memory_profile.hpp
    src/kll_sketch_memory_profile_impl.hpp
    src/kll_sketch_timing_profile.hpp
    src/kll_sketch_timing_profile_impl.hpp
    src/memory_usage_profile.cpp
    src/memory_usage_profile.hpp
    src/req_merge_timing_profile.hpp
    src/req_merge_timing_profile_impl.hpp
    src/req_sketch_timing_profile.hpp
    src/req_sketch_timing_profile_impl.hpp
    src/theta_sketch_accuracy_profile.cpp
    src/theta_sketch_accuracy_profile.hpp
    src/theta_sketch_memory_profile.cpp
    src/theta_sketch_memory_profile.hpp
    src/theta_sketch_timing_profile.cpp
    src/theta_sketch_timing_profile.hpp
    src/theta_union_accuracy_profile.cpp
    src/theta_union_accuracy_profile.hpp
    src/theta_union_timing_profile.cpp
    src/theta_union_timing_profile.hpp
    src/tuple_sketch_timing_profile.cpp
    src/tuple_sketch_timing_profile.hpp
    src/tuple_union_timing_profile.cpp
    src/tuple_union_timing_profile.hpp
    src/zipf_distribution.cpp
    src/zipf_distribution.hpp
)
