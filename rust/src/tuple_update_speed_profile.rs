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

use crate::job::JobProfile;
use crate::job::num_trials;
use crate::job::run_speed;
use crate::job::time_ns;
use crate::job::u_points;
use crate::theta_utils::ThetaJobConfig;
use crate::tuple_utils::DoubleSketch;
use crate::tuple_utils::double_sketch;

pub struct TupleUpdateSpeedProfile {
    config: ThetaJobConfig,
    sketch: DoubleSketch,
    v_in: u64,
}

impl TupleUpdateSpeedProfile {
    pub fn new(config: ThetaJobConfig) -> Self {
        Self {
            sketch: double_sketch(&config),
            config,
            v_in: 0,
        }
    }
}

impl JobProfile for TupleUpdateSpeedProfile {
    fn run(&mut self) {
        let c = self.config.clone();
        let (sketch, v_in) = (&mut self.sketch, &mut self.v_in);
        run_speed(
            "InU\tTrials\tnS/Set",
            u_points(c.lg_min_u, c.lg_max_u, c.uppo),
            |u| num_trials(u, c.lg_min_t, c.lg_max_t, c.lg_min_bpu, c.lg_max_bpu),
            |u| {
                sketch.reset();
                let (ns, ()) = time_ns(|| {
                    for _ in 0..u {
                        *v_in += 1;
                        sketch.update(*v_in, 0.0);
                    }
                });
                [ns / u as f64]
            },
        );
    }
}
