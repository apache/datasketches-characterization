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

use datasketches::theta::ThetaSketch;

use crate::accuracy::AccuracyStats;
use crate::accuracy::build_log2_accuracy_stats;
use crate::accuracy::run_accuracy;
use crate::job::JobProfile;
use crate::theta_utils::ThetaJobConfig;

pub struct ThetaAccuracyProfile {
    config: ThetaJobConfig,
    sketch: ThetaSketch,
    stats: Vec<AccuracyStats>,
}

impl ThetaAccuracyProfile {
    pub fn new(config: ThetaJobConfig) -> Self {
        let stats =
            build_log2_accuracy_stats(config.lg_min_u, config.lg_max_u, config.uppo, config.lg_qk);
        Self {
            sketch: config.sketch(),
            config,
            stats,
        }
    }
}

impl JobProfile for ThetaAccuracyProfile {
    fn run(&mut self) {
        let c = self.config.clone();
        let sketch = &mut self.sketch;
        run_accuracy(
            c.lg_min_t,
            c.lg_max_t,
            c.tppo,
            c.inter_data,
            1 << c.lg_max_u,
            &mut self.stats,
            false,
            &format!("{c:?}"),
            |stats, mut key| {
                sketch.reset();
                let mut last_uniques = 0;
                for stat in stats {
                    for _ in last_uniques..stat.true_value {
                        key += 1;
                        sketch.update(key);
                    }
                    last_uniques = stat.true_value;
                    stat.update(sketch);
                }
                key
            },
        );
    }
}
