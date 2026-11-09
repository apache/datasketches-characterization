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

use datasketches::common::ResizeFactor;
use datasketches::theta::ThetaSketch;
use datasketches::theta::ThetaSketchBuilder;

use crate::job::Overrides;

/// Theta job configuration; field meanings follow the Go and Java characterizations.
#[derive(Debug, Clone)]
pub struct ThetaJobConfig {
    /// The starting # of uniques that is printed at the end.
    pub lg_min_u: u32,
    /// How high the # uniques go.
    pub lg_max_u: u32,
    /// The horizontal x-resolution of trials points.
    pub uppo: u32,
    /// Prints intermediate results starting with this many trials.
    pub lg_min_t: u32,
    /// The max trials.
    pub lg_max_t: u32,
    /// How often intermediate results are printed.
    pub tppo: u32,
    /// Start the downward slope of trials at this U.
    pub lg_min_bpu: u32,
    /// Stop the downward slope of trials at this U.
    pub lg_max_bpu: u32,
    /// Print intermediate accuracy tables.
    pub inter_data: bool,
    /// Size of the quantiles sketch.
    pub lg_qk: u32,
    pub lg_k: u8,
    pub p: f32,
    pub resize_factor: u8,
    pub num_sketches: usize,
}

impl ThetaJobConfig {
    pub fn with_overrides(self, o: &Overrides) -> Self {
        Self {
            lg_min_u: o.get("lg_min_u", self.lg_min_u),
            lg_max_u: o.get("lg_max_u", self.lg_max_u),
            uppo: o.get("uppo", self.uppo),
            lg_min_t: o.get("lg_min_t", self.lg_min_t),
            lg_max_t: o.get("lg_max_t", self.lg_max_t),
            tppo: o.get("tppo", self.tppo),
            lg_min_bpu: o.get("lg_min_bpu", self.lg_min_bpu),
            lg_max_bpu: o.get("lg_max_bpu", self.lg_max_bpu),
            inter_data: o.get("inter_data", self.inter_data),
            lg_qk: o.get("lg_qk", self.lg_qk),
            lg_k: o.get("lg_k", self.lg_k),
            p: o.get("p", self.p),
            resize_factor: o.get("resize_factor", self.resize_factor),
            num_sketches: o.get("num_sketches", self.num_sketches),
        }
    }

    pub fn sketch(&self) -> ThetaSketch {
        ThetaSketchBuilder::default()
            .lg_k(self.lg_k)
            .sampling_probability(self.p)
            .resize_factor(resize_factor(self.resize_factor))
            .build()
            .unwrap()
    }
}

pub fn resize_factor(factor: u8) -> ResizeFactor {
    match factor {
        1 => ResizeFactor::X1,
        2 => ResizeFactor::X2,
        4 => ResizeFactor::X4,
        8 => ResizeFactor::X8,
        _ => panic!("resize_factor must be 1, 2, 4 or 8, got {factor}"),
    }
}
