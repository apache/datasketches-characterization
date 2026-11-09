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

use datasketches::tuple::DefaultUnionPolicy;
use datasketches::tuple::TupleANotB;
use datasketches::tuple::TupleIntersection;
use datasketches::tuple::TupleJaccardSimilarity;

use crate::job::JobProfile;
use crate::job::num_trials;
use crate::job::run_speed;
use crate::job::time_ns;
use crate::job::u_points;
use crate::theta_utils::ThetaJobConfig;
use crate::tuple_utils::double_sketch;
use crate::tuple_utils::double_union;

/// Times each set operation on two tuple update sketches of `InU` uniques that overlap by half,
/// including building the result. Inputs are untrimmed update sketches and every result is
/// requested ordered, so the timings include sorting the output.
pub struct TupleSetOperationsSpeedProfile {
    config: ThetaJobConfig,
    v_in: u64,
}

impl TupleSetOperationsSpeedProfile {
    pub fn new(config: ThetaJobConfig) -> Self {
        Self { config, v_in: 0 }
    }
}

impl JobProfile for TupleSetOperationsSpeedProfile {
    fn run(&mut self) {
        let c = self.config.clone();
        let v_in = &mut self.v_in;
        let a_not_b = TupleANotB::default();
        let jaccard = TupleJaccardSimilarity::default();
        run_speed(
            "InU\tTrials\tUnion_nS\tIntersection_nS\tANotB_nS\tJaccard_nS",
            u_points(c.lg_min_u, c.lg_max_u, c.uppo),
            |u| num_trials(u, c.lg_min_t, c.lg_max_t, c.lg_min_bpu, c.lg_max_bpu),
            |u| {
                let (mut a, mut b) = (double_sketch(&c), double_sketch(&c));
                let base = *v_in;
                for i in 0..u {
                    a.update(base + i, 1.0);
                    b.update(base + u / 2 + i, 1.0);
                }
                *v_in += u + u / 2;
                let (union_ns, _) = time_ns(|| {
                    let mut union = double_union(&c);
                    union.update(&a).unwrap();
                    union.update(&b).unwrap();
                    union.to_sketch(true)
                });
                let (intersection_ns, _) = time_ns(|| {
                    let mut intersection =
                        TupleIntersection::new(DefaultUnionPolicy::<f64>::default());
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
