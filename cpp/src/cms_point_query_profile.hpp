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

#ifndef CMS_POINT_QUERY_PROFILE_HPP_
#define CMS_POINT_QUERY_PROFILE_HPP_

#include "job_profile.hpp"

namespace datasketches {

/**
 * CMS point query error profile.
 *
 * For each of three Zipf skew settings (high=1.5, medium=1.0, low=0.5),
 * generates a fixed Zipf stream ONCE, then runs 8192 trials varying only
 * the CMS hash seed. Per-item absolute and relative errors are accumulated
 * in KLL sketches across trials.
 *
 * Output: one TSV per skew regime with per-frequency-rank error quantiles
 * at the 7 sigma levels, plus bound violation rate per item.
 */
class cms_point_query_profile: public job_profile {
public:
  void run() override;
};

}

#endif
