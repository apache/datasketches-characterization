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

use datasketches::theta::ThetaANotB;
use datasketches::theta::ThetaIntersection;
use datasketches::theta::ThetaJaccardSimilarity;
use datasketches::theta::ThetaUnionBuilder;

use crate::job::JobProfile;
use crate::job::num_trials;
use crate::job::run_speed;
use crate::job::time_ns;
use crate::job::u_points;
use crate::theta_utils::ThetaJobConfig;

/// Times each set operation on two update sketches of `InU` uniques that overlap by half,
/// including building the result.
pub struct ThetaSetOperationsSpeedProfile {
    config: ThetaJobConfig,
    v_in: u64,
}

impl ThetaSetOperationsSpeedProfile {
    pub fn new(config: ThetaJobConfig) -> Self {
        Self { config, v_in: 0 }
    }
}

impl JobProfile for ThetaSetOperationsSpeedProfile {
    fn run(&mut self) {
        let c = self.config.clone();
        let v_in = &mut self.v_in;
        let a_not_b = ThetaANotB::default();
        let jaccard = ThetaJaccardSimilarity::default();
        run_speed(
            "InU\tTrials\tUnion_nS\tIntersection_nS\tANotB_nS\tJaccard_nS",
            u_points(c.lg_min_u, c.lg_max_u, c.uppo),
            |u| num_trials(u, c.lg_min_t, c.lg_max_t, c.lg_min_bpu, c.lg_max_bpu),
            |u| {
                let (mut a, mut b) = (c.sketch(), c.sketch());
                let base = *v_in;
                for i in 0..u {
                    a.update(base + i);
                    b.update(base + u / 2 + i);
                }
                *v_in += u + u / 2;
                let (union_ns, _) = time_ns(|| {
                    let mut union = ThetaUnionBuilder::default().lg_k(c.lg_k).build().unwrap();
                    union.update(&a).unwrap();
                    union.update(&b).unwrap();
                    union.to_sketch(true)
                });
                let (intersection_ns, _) = time_ns(|| {
                    let mut intersection = ThetaIntersection::default();
                    intersection.update(&a).unwrap();
                    intersection.update(&b).unwrap();
                    intersection.to_sketch(true)
                });
                let (a_not_b_ns, _) = time_ns(|| a_not_b.compute(&a, &b, true).unwrap());
                let (jaccard_ns, _) = time_ns(|| jaccard.compute(&a, &b).unwrap());
                [union_ns, intersection_ns, a_not_b_ns, jaccard_ns]
            },
        );
    }
}
