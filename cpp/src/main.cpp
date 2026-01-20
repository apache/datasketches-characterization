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

#include "hll_sketch_timing_profile.hpp"
#include "hll_union_timing_profile.hpp"

#include "theta_sketch_timing_profile.hpp"
#include "theta_union_timing_profile.hpp"

#include "tuple_sketch_timing_profile.hpp"
#include "tuple_union_timing_profile.hpp"

#include "kll_sketch_timing_profile.hpp"
#include "kll_merge_timing_profile.hpp"

#include "frequent_items_sketch_timing_profile.hpp"
#include "frequent_items_merge_timing_profile.hpp"

#include "kll_sketch_accuracy_profile.hpp"
#include "kll_merge_accuracy_profile.hpp"

#include "frequent_items_sketch_accuracy_profile.hpp"

#include "cpc_sketch_accuracy_profile.hpp"
#include "cpc_union_accuracy_profile.hpp"

#include "hll_sketch_accuracy_profile.hpp"
#include "hll_union_accuracy_profile.hpp"

#include "theta_sketch_accuracy_profile.hpp"
#include "theta_union_accuracy_profile.hpp"

#include "tdigest_timing_profile.hpp"
#include "tdigest_merge_timing_profile.hpp"
#include "tdigest_accuracy_profile_impl.hpp"
#include "tdigest_sketch_accuracy_profile.hpp"
#include "tdigest_merge_accuracy_profile.hpp"
#include "tdigest_memory_profile.hpp"

#include "cpc_sketch_memory_profile.hpp"
#include "hll_sketch_memory_profile.hpp"
#include "theta_sketch_memory_profile.hpp"
#include "kll_sketch_memory_profile.hpp"

#include "hll_cross_language_profile.hpp"

#include "req_sketch_timing_profile.hpp"
#include "req_merge_timing_profile.hpp"
#include "req_error_vs_rank_profile.hpp"

using namespace datasketches;
typedef std::unique_ptr<job_profile> job_profile_ptr;

int main(int argc, char **argv) {
  job_profile::add("cpc-sketch-timing", job_profile_ptr(new cpc_sketch_timing_profile()));
  job_profile::add("cpc-union-timing", job_profile_ptr(new cpc_union_timing_profile()));
  job_profile::add("hll-sketch-timing", job_profile_ptr(new hll_sketch_timing_profile()));
  job_profile::add("hll-union-timing", job_profile_ptr(new hll_union_timing_profile()));
  job_profile::add("theta-sketch-timing", job_profile_ptr(new theta_sketch_timing_profile()));
  job_profile::add("theta-union-timing", job_profile_ptr(new theta_union_timing_profile()));
  job_profile::add("tuple-sketch-timing", job_profile_ptr(new tuple_sketch_timing_profile()));
  job_profile::add("tuple-union-timing", job_profile_ptr(new tuple_union_timing_profile()));
  job_profile::add("kll-sketch-timing-float", job_profile_ptr(new kll_sketch_timing_profile<float>()));
  job_profile::add("kll-sketch-timing-string", job_profile_ptr(new kll_sketch_timing_profile<std::string>()));
  job_profile::add("kll-merge-timing-float", job_profile_ptr(new kll_merge_timing_profile<float>()));
  job_profile::add("kll-merge-timing-string", job_profile_ptr(new kll_merge_timing_profile<std::string>()));
  job_profile::add("fi-sketch-timing", job_profile_ptr(new frequent_items_sketch_timing_profile()));
  job_profile::add("fi-merge-timing", job_profile_ptr(new frequent_items_merge_timing_profile()));
  job_profile::add("req-sketch-timing-float", job_profile_ptr(new req_sketch_timing_profile<float>()));
  job_profile::add("req-merge-timing-float", job_profile_ptr(new req_merge_timing_profile<float>()));
  job_profile::add("req-sketch-timing-double", job_profile_ptr(new req_sketch_timing_profile<double>()));

  job_profile::add("cpc-sketch-accuracy", job_profile_ptr(new cpc_sketch_accuracy_profile()));
  job_profile::add("cpc-union-accuracy", job_profile_ptr(new cpc_union_accuracy_profile()));
  job_profile::add("hll-sketch-accuracy", job_profile_ptr(new hll_sketch_accuracy_profile()));
  job_profile::add("hll-union-accuracy", job_profile_ptr(new hll_union_accuracy_profile()));
  job_profile::add("theta-sketch-accuracy", job_profile_ptr(new theta_sketch_accuracy_profile()));
  job_profile::add("theta-union-accuracy", job_profile_ptr(new theta_union_accuracy_profile()));
  job_profile::add("kll-sketch-accuracy", job_profile_ptr(new kll_sketch_accuracy_profile()));
  job_profile::add("kll-merge-accuracy", job_profile_ptr(new kll_merge_accuracy_profile()));
  job_profile::add("fi-sketch-accuracy", job_profile_ptr(new frequent_items_sketch_accuracy_profile()));
  job_profile::add("req-error-vs-rank-double", job_profile_ptr(new req_error_vs_rank_profile<double>()));

  job_profile::add("tdigest-timing-double", job_profile_ptr(new tdigest_timing_profile<double>()));
  job_profile::add("tdigest-merge-timing-double", job_profile_ptr(new tdigest_merge_timing_profile<double>()));
  job_profile::add("tdigest-sketch-accuracy-double", job_profile_ptr(new tdigest_sketch_accuracy_profile<double>()));
  job_profile::add("tdigest-merge-accuracy-double", job_profile_ptr(new tdigest_merge_accuracy_profile<double>()));
  job_profile::add("tdigest-timing-float", job_profile_ptr(new tdigest_timing_profile<float>()));
  job_profile::add("tdigest-sketch-accuracy-float", job_profile_ptr(new tdigest_sketch_accuracy_profile<float>()));
  job_profile::add("tdigest-merge-accuracy-float", job_profile_ptr(new tdigest_merge_accuracy_profile<float>()));
  job_profile::add("tdigest-memory-float", job_profile_ptr(new tdigest_memory_profile<float>()));
  job_profile::add("tdigest-memory-double", job_profile_ptr(new tdigest_memory_profile<double>()));

  job_profile::add("cpc-sketch-memory", job_profile_ptr(new cpc_sketch_memory_profile()));
  job_profile::add("hll-sketch-memory", job_profile_ptr(new hll_sketch_memory_profile()));
  job_profile::add("theta-sketch-memory", job_profile_ptr(new theta_sketch_memory_profile()));
  job_profile::add("kll-sketch-memory-float", job_profile_ptr(new kll_sketch_memory_profile<float>()));
  job_profile::add("kll-sketch-memory-int64", job_profile_ptr(new kll_sketch_memory_profile<int64_t>()));

  job_profile::add("hll-cross-lang", job_profile_ptr(new hll_cross_language_profile()));

  if (argc == 2) {
    datasketches::job_profile& profile = datasketches::job_profile::instance(argv[1]);
    profile.run();
  } else {
    std::cerr << "One parameter expected: profile name" << std::endl;
    std::cerr << "Known profiles:" << std::endl;

    std::vector<std::string> profile_names = job_profile::get_profile_names();
    for (std::string& name : profile_names) {
      std::cerr << "\t" << name << std::endl;
    }
  }
  return 0;
}
