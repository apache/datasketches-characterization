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

use datasketches::theta::CompactThetaSketch;
use datasketches::theta::ThetaSketchBuilder;
use datasketches::theta::ThetaUnionBuilder;

use crate::job::JobProfile;
use crate::job::num_trials;
use crate::job::run_speed;
use crate::job::time_ns;
use crate::job::u_points;
use crate::theta_utils::ThetaJobConfig;

pub struct ThetaUnionUpdateSpeedProfile {
    config: ThetaJobConfig,
    v_in: u64,
}

impl ThetaUnionUpdateSpeedProfile {
    pub fn new(config: ThetaJobConfig) -> Self {
        Self { config, v_in: 0 }
    }
}

impl JobProfile for ThetaUnionUpdateSpeedProfile {
    fn run(&mut self) {
        let c = self.config.clone();
        let v_in = &mut self.v_in;
        run_speed(
            "InU\tTrials\tnS/Set",
            u_points(c.lg_min_u, c.lg_max_u, c.uppo),
            |u| num_trials(u, c.lg_min_t, c.lg_max_t, c.lg_min_bpu, c.lg_max_bpu),
            |u| {
                let mut sketches: Vec<_> = (0..c.num_sketches)
                    .map(|_| ThetaSketchBuilder::default().lg_k(c.lg_k).build().unwrap())
                    .collect();
                for i in 0..u as usize {
                    *v_in += 1;
                    sketches[i % c.num_sketches].update(*v_in);
                }
                let compacts: Vec<CompactThetaSketch> = sketches
                    .iter_mut()
                    .map(|sketch| {
                        sketch.trim();
                        sketch.compact(true)
                    })
                    .collect();
                let mut union = ThetaUnionBuilder::default().lg_k(c.lg_k).build().unwrap();
                let (ns, ()) = time_ns(|| {
                    for sketch in &compacts {
                        union.update(sketch).unwrap();
                    }
                });
                [ns]
            },
        );
    }
}
