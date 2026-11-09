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
use datasketches::tuple::DefaultUpdatePolicy;
use datasketches::tuple::TupleSketch;
use datasketches::tuple::TupleSketchBuilder;
use datasketches::tuple::TupleUnion;
use datasketches::tuple::TupleUnionBuilder;

use crate::theta_utils::ThetaJobConfig;
use crate::theta_utils::resize_factor;

/// A tuple sketch with one double per key, the Rust counterpart of a single-value
/// array-of-doubles sketch.
pub type DoubleSketch = TupleSketch<DefaultUpdatePolicy<f64>>;

pub fn double_sketch(config: &ThetaJobConfig) -> DoubleSketch {
    TupleSketchBuilder::new(DefaultUpdatePolicy::<f64>::default())
        .lg_k(config.lg_k)
        .sampling_probability(config.p)
        .resize_factor(resize_factor(config.resize_factor))
        .build()
        .unwrap()
}

pub fn double_union(config: &ThetaJobConfig) -> TupleUnion<DefaultUnionPolicy<f64>> {
    TupleUnionBuilder::new(DefaultUnionPolicy::<f64>::default())
        .lg_k(config.lg_k)
        .build()
        .unwrap()
}
