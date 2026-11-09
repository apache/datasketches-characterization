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

use datasketches::cpc::CpcSketch;

use crate::job::JobProfile;
use crate::job::Overrides;
use crate::job::num_trials;
use crate::job::run_speed;
use crate::job::time_ns;
use crate::job::u_points;

#[derive(Debug, Clone)]
pub struct CpcJobConfig {
    pub lg_min_u: u32,
    pub lg_max_u: u32,
    pub uppo: u32,
    pub lg_min_t: u32,
    pub lg_max_t: u32,
    pub lg_min_bpu: u32,
    pub lg_max_bpu: u32,
    pub lg_k: u8,
}

impl CpcJobConfig {
    pub fn with_overrides(self, o: &Overrides) -> Self {
        Self {
            lg_min_u: o.get("lg_min_u", self.lg_min_u),
            lg_max_u: o.get("lg_max_u", self.lg_max_u),
            uppo: o.get("uppo", self.uppo),
            lg_min_t: o.get("lg_min_t", self.lg_min_t),
            lg_max_t: o.get("lg_max_t", self.lg_max_t),
            lg_min_bpu: o.get("lg_min_bpu", self.lg_min_bpu),
            lg_max_bpu: o.get("lg_max_bpu", self.lg_max_bpu),
            lg_k: o.get("lg_k", self.lg_k),
        }
    }
}

/// Times updating a new CPC sketch with `InU` uniques; Rust has no `reset`, so each trial creates
/// the sketch outside the timed section.
pub struct CpcUpdateSpeedProfile {
    config: CpcJobConfig,
    v_in: u64,
}

impl CpcUpdateSpeedProfile {
    pub fn new(config: CpcJobConfig) -> Self {
        Self { config, v_in: 0 }
    }
}

impl JobProfile for CpcUpdateSpeedProfile {
    fn run(&mut self) {
        let c = self.config.clone();
        let v_in = &mut self.v_in;
        run_speed(
            "InU\tTrials\tnS/u",
            u_points(c.lg_min_u, c.lg_max_u, c.uppo),
            |u| num_trials(u, c.lg_min_t, c.lg_max_t, c.lg_min_bpu, c.lg_max_bpu),
            |u| {
                let mut sketch = CpcSketch::new(c.lg_k).unwrap();
                let (ns, ()) = time_ns(|| {
                    for _ in 0..u {
                        *v_in += 1;
                        sketch.update(*v_in);
                    }
                });
                [ns / u as f64]
            },
        );
    }
}
