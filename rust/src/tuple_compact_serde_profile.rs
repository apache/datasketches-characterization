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

use datasketches::tuple::CompactTupleSketch;

use crate::job::JobProfile;
use crate::job::num_trials;
use crate::job::run_speed;
use crate::job::time_ns;
use crate::job::u_points;
use crate::theta_utils::ThetaJobConfig;
use crate::tuple_utils::DoubleSketch;
use crate::tuple_utils::double_sketch;

/// Times compaction, serialization and estimation of one tuple sketch per trial.
///
/// Ordered and unordered compaction alternate which runs first so neither always sees warm
/// caches.
pub struct TupleCompactSerDeProfile {
    config: ThetaJobConfig,
    sketch: DoubleSketch,
    v_in: u64,
    ordered_first: bool,
}

impl TupleCompactSerDeProfile {
    pub fn new(config: ThetaJobConfig) -> Self {
        Self {
            sketch: double_sketch(&config),
            config,
            v_in: 0,
            ordered_first: false,
        }
    }
}

impl JobProfile for TupleCompactSerDeProfile {
    fn run(&mut self) {
        let c = self.config.clone();
        let (sketch, v_in, ordered_first) =
            (&mut self.sketch, &mut self.v_in, &mut self.ordered_first);
        run_speed(
            "TrueU\tTrials\tCompact_nS\tCompactUnordered_nS\tSer_nS\tDeSer_nS\tEst_nS\tSize_B",
            u_points(c.lg_min_u, c.lg_max_u, c.uppo),
            |u| num_trials(u, c.lg_min_t, c.lg_max_t, c.lg_min_bpu, c.lg_max_bpu),
            |u| {
                sketch.reset();
                for _ in 0..u {
                    *v_in += 1;
                    sketch.update(*v_in, 1.0);
                }
                let (est_ns, est) = time_ns(|| sketch.estimate());
                *ordered_first = !*ordered_first;
                let ((compact_ns, compact), (compact_u_ns, _)) = if *ordered_first {
                    let ordered = time_ns(|| sketch.compact(true));
                    (ordered, time_ns(|| sketch.compact(false)))
                } else {
                    let unordered = time_ns(|| sketch.compact(false));
                    (time_ns(|| sketch.compact(true)), unordered)
                };
                let (ser_ns, bytes) = time_ns(|| compact.serialize());
                let (de_ns, de) =
                    time_ns(|| CompactTupleSketch::<f64>::deserialize(&bytes).unwrap());
                assert_eq!(de.estimate(), est, "estimate mismatch after deserialize");
                [
                    compact_ns,
                    compact_u_ns,
                    ser_ns,
                    de_ns,
                    est_ns,
                    bytes.len() as f64,
                ]
            },
        );
    }
}
