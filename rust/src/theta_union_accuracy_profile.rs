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
use datasketches::theta::ThetaUnion;
use datasketches::theta::ThetaUnionBuilder;

use crate::accuracy::AccuracyStats;
use crate::accuracy::build_log2_accuracy_stats;
use crate::accuracy::run_accuracy;
use crate::job::JobProfile;
use crate::theta_utils::ThetaJobConfig;
use crate::theta_utils::resize_factor;

pub struct ThetaUnionAccuracyProfile {
    config: ThetaJobConfig,
    union: ThetaUnion,
    sketch: ThetaSketch,
    stats: Vec<AccuracyStats>,
}

impl ThetaUnionAccuracyProfile {
    pub fn new(config: ThetaJobConfig) -> Self {
        let union = ThetaUnionBuilder::default()
            .lg_k(config.lg_k)
            .sampling_probability(config.p)
            .resize_factor(resize_factor(config.resize_factor))
            .build()
            .unwrap();
        let stats =
            build_log2_accuracy_stats(config.lg_min_u, config.lg_max_u, config.uppo, config.lg_qk);
        Self {
            sketch: config.sketch(),
            union,
            config,
            stats,
        }
    }
}

impl JobProfile for ThetaUnionAccuracyProfile {
    fn run(&mut self) {
        let c = self.config.clone();
        let (union, sketch) = (&mut self.union, &mut self.sketch);
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
                union.reset();
                let mut last_uniques = 0;
                for stat in stats {
                    sketch.reset();
                    for _ in last_uniques..stat.true_value {
                        key += 1;
                        sketch.update(key);
                    }
                    last_uniques = stat.true_value;
                    union.update(&*sketch).unwrap();
                    stat.update(&union.to_sketch(true));
                }
                key
            },
        );
    }
}
