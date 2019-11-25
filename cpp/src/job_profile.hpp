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

#ifndef JOB_PROFILE_HPP_
#define JOB_PROFILE_HPP_

#include <memory>
#include <string>
#include <unordered_map>

namespace datasketches {

class job_profile {
public:
   virtual ~job_profile() {}

   static void add(const char* name, std::unique_ptr<job_profile> profile);
   static const job_profile& instance(const char* name);

   virtual void run() const = 0;

   static size_t pwr_2_law_next(size_t ppo, size_t cur_point);
   static size_t count_points(size_t lg_start, size_t lg_end, size_t ppo);
   static size_t get_num_trials(size_t x, size_t lg_min_x, size_t lg_max_x, size_t lg_min_trials, size_t lg_max_trials);

private:
   static std::unordered_map<std::string, std::unique_ptr<job_profile>> registry;
};

}

#endif
