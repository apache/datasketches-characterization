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

#ifndef HLL_CROSS_LANGUAGE_PROFILE_HPP_
#define HLL_CROSS_LANGUAGE_PROFILE_HPP_

#include "job_profile.hpp"

namespace datasketches {

class hll_cross_language_profile: public job_profile {
public:
  void run();
private:
  const std::string DATA_PATH = "../src/main/resources/hll/data";
  const int BASE_LG_K = 8;
  const std::string MODE_ARR[4] = {"Empty", "List", "Set", "Hll"};
  const int LG_K_SEQ_ARR[4] = {0, 1, 5, 6};
};

}

#endif
