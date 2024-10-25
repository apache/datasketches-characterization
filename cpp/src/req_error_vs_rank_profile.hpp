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

#ifndef REQ_ERROR_VS_RANK_PROFILE_HPP_
#define REQ_ERROR_VS_RANK_PROFILE_HPP_

#include <random>

#include "job_profile.hpp"

namespace datasketches {

template<typename T>
class req_error_vs_rank_profile: public job_profile {
public:
  req_error_vs_rank_profile();
  void run();
  T sample();
private:
  std::default_random_engine generator;
  std::uniform_real_distribution<double> distribution;
};

}

#include "req_error_vs_rank_profile_impl.hpp"

#endif
