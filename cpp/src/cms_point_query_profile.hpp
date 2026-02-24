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
 * CMS point query error profile (Experiment 1).
 *
 * Runs many trials over a fixed Zipf distribution. For each trial,
 * builds a fresh CMS, inserts the stream, and computes the absolute
 * error (estimate - true count) for every item. Errors are accumulated
 * in per-item KLL quantile sketches across all trials.
 *
 * Output: one TSV row per item (sorted by mean frequency ascending)
 * with quantile columns at -3s, -2s, -1s, median, +1s, +2s, +3s.
 */
class cms_point_query_profile: public job_profile {
public:
  void run() override;
};

}

#endif
