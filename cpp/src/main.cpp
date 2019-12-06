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

#include "job_profile.hpp"

#include "cpc_sketch_timing_profile.hpp"
#include "cpc_union_timing_profile.hpp"

#include "theta_sketch_timing_profile.hpp"
#include "theta_union_timing_profile.hpp"

#include "kll_sketch_timing_profile.hpp"
#include "frequent_items_sketch_timing_profile.hpp"
#include "hll_union_timing_profile.hpp"

#include "kll_sketch_accuracy_profile.hpp"
#include "kll_merge_accuracy_profile.hpp"

#include "frequent_items_sketch_accuracy_profile.hpp"

#include "cpc_sketch_accuracy_profile.hpp"
#include "cpc_union_accuracy_profile.hpp"

#include "hll_sketch_accuracy_profile.hpp"
#include "hll_union_accuracy_profile.hpp"

#include "theta_sketch_accuracy_profile.hpp"
#include "theta_union_accuracy_profile.hpp"

using namespace datasketches;
typedef std::unique_ptr<job_profile> job_profile_ptr;

int main(int argc, char **argv) {
  job_profile::add("cpc-sketch-timing", job_profile_ptr(new cpc_sketch_timing_profile()));
  job_profile::add("cpc-union-timing", job_profile_ptr(new cpc_union_timing_profile()));
  job_profile::add("theta-sketch-timing", job_profile_ptr(new theta_sketch_timing_profile()));
  job_profile::add("theta-union-timing", job_profile_ptr(new theta_union_timing_profile()));
  job_profile::add("kll-timing", job_profile_ptr(new kll_sketch_timing_profile()));
  job_profile::add("fi-timing", job_profile_ptr(new frequent_items_sketch_timing_profile()));
  job_profile::add("hll-union-timing", job_profile_ptr(new hll_union_timing_profile()));

  job_profile::add("kll-sketch-accuracy", job_profile_ptr(new kll_sketch_accuracy_profile()));
  job_profile::add("kll-merge-accuracy", job_profile_ptr(new kll_merge_accuracy_profile()));
  job_profile::add("fi-sketch-accuracy", job_profile_ptr(new frequent_items_sketch_accuracy_profile()));
  job_profile::add("cpc-sketch-accuracy", job_profile_ptr(new cpc_sketch_accuracy_profile()));
  job_profile::add("cpc-union-accuracy", job_profile_ptr(new cpc_union_accuracy_profile()));
  job_profile::add("hll-sketch-accuracy", job_profile_ptr(new hll_sketch_accuracy_profile()));
  job_profile::add("hll-union-accuracy", job_profile_ptr(new hll_sketch_accuracy_profile()));
  job_profile::add("theta-sketch-accuracy", job_profile_ptr(new theta_sketch_accuracy_profile()));
  job_profile::add("theta-union-accuracy", job_profile_ptr(new theta_union_accuracy_profile()));

  if (argc == 2) {
    datasketches::job_profile& profile = datasketches::job_profile::instance(argv[1]);
    profile.run();
  } else {
    std::cerr << "One parameter expected: profile name" << std::endl;
  }
  return 0;
}
