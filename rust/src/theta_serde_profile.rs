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
use datasketches::theta::ThetaSketch;

use crate::job::JobProfile;
use crate::job::num_trials;
use crate::job::run_speed;
use crate::job::time_ns;
use crate::job::u_points;
use crate::theta_utils::ThetaJobConfig;

/// Times compaction, both serialization formats and estimation of one sketch per trial.
pub struct ThetaSerDeProfile {
    config: ThetaJobConfig,
    sketch: ThetaSketch,
    v_in: u64,
}

impl ThetaSerDeProfile {
    pub fn new(config: ThetaJobConfig) -> Self {
        Self {
            sketch: config.sketch(),
            config,
            v_in: 0,
        }
    }
}

impl JobProfile for ThetaSerDeProfile {
    fn run(&mut self) {
        let c = self.config.clone();
        let (sketch, v_in) = (&mut self.sketch, &mut self.v_in);
        run_speed(
            "TrueU\tTrials\tCompact_nS\tCompactUnordered_nS\tSer_nS\tDeSer_nS\tSerCompressed_nS\tDeSerCompressed_nS\tEst_nS\tSize_B\tSizeCompressed_B",
            u_points(c.lg_min_u, c.lg_max_u, c.uppo),
            |u| num_trials(u, c.lg_min_t, c.lg_max_t, c.lg_min_bpu, c.lg_max_bpu),
            |u| {
                sketch.reset();
                for _ in 0..u {
                    *v_in += 1;
                    sketch.update(*v_in);
                }
                let (est_ns, est) = time_ns(|| sketch.estimate());
                let (compact_u_ns, _) = time_ns(|| sketch.compact(false));
                let (compact_ns, compact) = time_ns(|| sketch.compact(true));
                let (ser_ns, bytes) = time_ns(|| compact.serialize());
                let (de_ns, de) = time_ns(|| CompactThetaSketch::deserialize(&bytes).unwrap());
                let (ser_c_ns, bytes_c) = time_ns(|| compact.serialize_compressed());
                let (de_c_ns, de_c) =
                    time_ns(|| CompactThetaSketch::deserialize(&bytes_c).unwrap());
                assert_eq!(de.estimate(), est, "estimate mismatch after deserialize");
                assert_eq!(de_c.estimate(), est, "estimate mismatch after compressed deserialize");
                [
                    compact_ns,
                    compact_u_ns,
                    ser_ns,
                    de_ns,
                    ser_c_ns,
                    de_c_ns,
                    est_ns,
                    bytes.len() as f64,
                    bytes_c.len() as f64,
                ]
            },
        );
    }
}
