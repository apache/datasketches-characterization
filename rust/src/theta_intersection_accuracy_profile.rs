// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datasketches::theta::ThetaIntersection;
use datasketches::theta::ThetaSketch;

use crate::accuracy::AccuracyStats;
use crate::accuracy::build_log2_intersection_accuracy_stats;
use crate::accuracy::run_accuracy;
use crate::job::JobProfile;
use crate::theta_utils::ThetaJobConfig;

pub struct ThetaIntersectionAccuracyProfile {
    config: ThetaJobConfig,
    small: ThetaSketch,
    large: ThetaSketch,
    stats: Vec<AccuracyStats>,
}

impl ThetaIntersectionAccuracyProfile {
    pub fn new(config: ThetaJobConfig) -> Self {
        let stats = build_log2_intersection_accuracy_stats(
            config.lg_min_u,
            config.lg_max_u,
            config.uppo,
            config.lg_qk,
        );
        Self {
            small: config.sketch(),
            large: config.sketch(),
            config,
            stats,
        }
    }
}

impl JobProfile for ThetaIntersectionAccuracyProfile {
    fn run(&mut self) {
        let c = self.config.clone();
        let (small, large) = (&mut self.small, &mut self.large);
        run_accuracy(
            c.lg_min_t,
            c.lg_max_t,
            c.tppo,
            c.inter_data,
            1 << c.lg_max_u,
            &mut self.stats,
            true,
            &format!("{c:?}"),
            |stats, mut key| {
                small.reset();
                large.reset();
                let mut last_uniques = 0;
                for (i, stat) in stats.iter_mut().enumerate() {
                    for _ in last_uniques..stat.num_of_unique {
                        key += 1;
                        if i == 0 {
                            small.update(key);
                        }
                        large.update(key);
                    }
                    last_uniques = stat.num_of_unique;
                    small.trim();
                    large.trim();
                    let mut intersection = ThetaIntersection::default();
                    intersection.update(&*large).unwrap();
                    intersection.update(&*small).unwrap();
                    stat.update(&intersection.to_sketch(true).unwrap());
                }
                key
            },
        );
    }
}
