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

mod accuracy;
mod cpc_update_speed_profile;
mod job;
mod theta_accuracy_profile;
mod theta_intersection_accuracy_profile;
mod theta_serde_profile;
mod theta_set_operations_speed_profile;
mod theta_union_accuracy_profile;
mod theta_union_update_speed_profile;
mod theta_update_speed_profile;
mod theta_utils;
mod tuple_serde_profile;
mod tuple_set_operations_speed_profile;
mod tuple_union_update_speed_profile;
mod tuple_update_speed_profile;
mod tuple_utils;

use cpc_update_speed_profile::CpcJobConfig;
use cpc_update_speed_profile::CpcUpdateSpeedProfile;
use job::JobProfile;
use job::Overrides;
use theta_accuracy_profile::ThetaAccuracyProfile;
use theta_intersection_accuracy_profile::ThetaIntersectionAccuracyProfile;
use theta_serde_profile::ThetaSerDeProfile;
use theta_set_operations_speed_profile::ThetaSetOperationsSpeedProfile;
use theta_union_accuracy_profile::ThetaUnionAccuracyProfile;
use theta_union_update_speed_profile::ThetaUnionUpdateSpeedProfile;
use theta_update_speed_profile::ThetaUpdateSpeedProfile;
use theta_utils::ThetaJobConfig;
use tuple_serde_profile::TupleSerDeProfile;
use tuple_set_operations_speed_profile::TupleSetOperationsSpeedProfile;
use tuple_union_update_speed_profile::TupleUnionUpdateSpeedProfile;
use tuple_update_speed_profile::TupleUpdateSpeedProfile;

type JobFactory = fn(&Overrides) -> Box<dyn JobProfile>;

const THETA_SPEED: ThetaJobConfig = ThetaJobConfig {
    lg_min_u: 0,
    lg_max_u: 23,
    uppo: 16,
    lg_min_t: 4,
    lg_max_t: 23,
    tppo: 1,
    lg_min_bpu: 4,
    lg_max_bpu: 20,
    inter_data: false,
    lg_qk: 12,
    lg_k: 12,
    p: 1.0,
    resize_factor: 1,
    num_sketches: 32,
};

const THETA_UNION_SPEED: ThetaJobConfig = ThetaJobConfig {
    lg_max_u: 24,
    lg_min_t: 6,
    lg_max_t: 14,
    lg_min_bpu: 0,
    lg_max_bpu: 24,
    resize_factor: 8,
    ..THETA_SPEED
};

const SERDE: ThetaJobConfig = ThetaJobConfig {
    lg_max_u: 20,
    uppo: 2,
    lg_min_t: 12,
    lg_max_t: 16,
    lg_min_bpu: 0,
    resize_factor: 2,
    ..THETA_SPEED
};

const SET_OPERATIONS_SPEED: ThetaJobConfig = ThetaJobConfig {
    lg_max_u: 20,
    uppo: 4,
    lg_min_t: 8,
    lg_max_t: 16,
    lg_min_bpu: 0,
    resize_factor: 8,
    ..THETA_SPEED
};

const THETA_ACCURACY: ThetaJobConfig = ThetaJobConfig {
    lg_min_u: 0,
    lg_max_u: 20,
    uppo: 16,
    lg_min_t: 8,
    lg_max_t: 18,
    tppo: 1,
    inter_data: true,
    lg_qk: 12,
    ..THETA_SPEED
};

fn jobs() -> Vec<(&'static str, JobFactory)> {
    vec![
        ("theta_accuracy_profile", |o| {
            Box::new(ThetaAccuracyProfile::new(THETA_ACCURACY.with_overrides(o)))
        }),
        ("theta_update_speed_profile", |o| {
            Box::new(ThetaUpdateSpeedProfile::new(THETA_SPEED.with_overrides(o)))
        }),
        ("theta_union_accuracy_profile", |o| {
            let config = ThetaJobConfig {
                lg_max_u: 24,
                lg_max_t: 10,
                lg_qk: 10,
                lg_k: 19,
                ..THETA_ACCURACY
            };
            Box::new(ThetaUnionAccuracyProfile::new(config.with_overrides(o)))
        }),
        ("theta_union_update_speed_profile", |o| {
            let config = ThetaJobConfig {
                resize_factor: 1,
                ..THETA_UNION_SPEED
            };
            Box::new(ThetaUnionUpdateSpeedProfile::new(config.with_overrides(o)))
        }),
        ("theta_intersection_accuracy_profile", |o| {
            let config = ThetaJobConfig {
                lg_min_u: 16,
                lg_max_u: 30,
                uppo: 1,
                lg_min_t: 2,
                lg_max_t: 9,
                tppo: 4,
                ..THETA_ACCURACY
            };
            Box::new(ThetaIntersectionAccuracyProfile::new(config.with_overrides(o)))
        }),
        ("theta_serde_profile", |o| {
            Box::new(ThetaSerDeProfile::new(SERDE.with_overrides(o)))
        }),
        ("theta_set_operations_speed_profile", |o| {
            Box::new(ThetaSetOperationsSpeedProfile::new(
                SET_OPERATIONS_SPEED.with_overrides(o),
            ))
        }),
        ("tuple_update_speed_profile", |o| {
            let config = ThetaJobConfig {
                resize_factor: 8,
                ..THETA_SPEED
            };
            Box::new(TupleUpdateSpeedProfile::new(config.with_overrides(o)))
        }),
        ("tuple_union_update_speed_profile", |o| {
            Box::new(TupleUnionUpdateSpeedProfile::new(
                THETA_UNION_SPEED.with_overrides(o),
            ))
        }),
        ("tuple_serde_profile", |o| {
            Box::new(TupleSerDeProfile::new(SERDE.with_overrides(o)))
        }),
        ("tuple_set_operations_speed_profile", |o| {
            Box::new(TupleSetOperationsSpeedProfile::new(
                SET_OPERATIONS_SPEED.with_overrides(o),
            ))
        }),
        ("cpc_update_speed_profile", |o| {
            let config = CpcJobConfig {
                lg_min_u: 0,
                lg_max_u: 20,
                uppo: 16,
                lg_min_t: 4,
                lg_max_t: 23,
                lg_min_bpu: 4,
                lg_max_bpu: 20,
                lg_k: 11,
            };
            Box::new(CpcUpdateSpeedProfile::new(config.with_overrides(o)))
        }),
    ]
}

fn usage() -> ! {
    println!("Usage: datasketches-characterization-rust <job> [name=value ...]");
    println!("Available jobs:");
    for (name, _) in jobs() {
        println!("\t{name}");
    }
    std::process::exit(1);
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(name) = args.first() else { usage() };
    let Some((_, factory)) = jobs().into_iter().find(|(job, _)| job == name) else {
        usage()
    };
    factory(&Overrides::parse(&args[1..])).run();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_job_runs_with_a_small_config() {
        let small: Vec<String> = [
            "lg_min_u=0",
            "lg_max_u=6",
            "uppo=2",
            "lg_min_t=1",
            "lg_max_t=2",
            "lg_min_bpu=0",
            "lg_max_bpu=6",
            "lg_qk=4",
            "lg_k=5",
        ]
        .map(String::from)
        .into();
        for (name, factory) in jobs() {
            let mut overrides = small.clone();
            if name == "theta_intersection_accuracy_profile" {
                overrides.push("lg_min_u=2".into());
            }
            factory(&Overrides::parse(&overrides)).run();
        }
    }
}
