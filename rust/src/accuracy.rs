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

use std::fmt::Write;
use std::time::Instant;

use datasketches::common::NumStdDev;
use datasketches::common::SearchCriteria;
use datasketches::kll::KllFloat;
use datasketches::kll::KllSketch;

use crate::job::count_points;
use crate::job::pwr2_series_next;

const M3SD: f64 = 0.0013498980316301;
const M2SD: f64 = 0.0227501319481792;
const M1SD: f64 = 0.1586552539314570;
const MED: f64 = 0.5;
const P1SD: f64 = 0.8413447460685430;
const P2SD: f64 = 0.9772498680518210;
const P3SD: f64 = 0.9986501019683700;
pub const GAUSSIANS_3SD: [f64; 9] = [0.0, M3SD, M2SD, M1SD, MED, P1SD, P2SD, P3SD, 1.0];

/// Estimate and bounds of one sketch result.
pub trait Estimator {
    fn estimate(&self) -> f64;
    fn lower_bound(&self, num_std_dev: NumStdDev) -> f64;
    fn upper_bound(&self, num_std_dev: NumStdDev) -> f64;
}

macro_rules! impl_estimator {
    ($($ty:ty),*) => {$(
        impl Estimator for $ty {
            fn estimate(&self) -> f64 { <$ty>::estimate(self) }
            fn lower_bound(&self, n: NumStdDev) -> f64 { <$ty>::lower_bound(self, n) }
            fn upper_bound(&self, n: NumStdDev) -> f64 { <$ty>::upper_bound(self, n) }
        }
    )*};
}

impl_estimator!(
    datasketches::theta::ThetaSketch,
    datasketches::theta::CompactThetaSketch
);

pub struct AccuracyStats {
    qsk: KllSketch<KllFloat<f64>>,
    sum_lb3: f64,
    sum_lb2: f64,
    sum_lb1: f64,
    sum_ub1: f64,
    sum_ub2: f64,
    sum_ub3: f64,
    sum_est: f64,
    sum_rel_err: f64,
    sum_sq_err: f64,
    pub true_value: u64,
    pub num_of_unique: u64,
}

impl AccuracyStats {
    fn new(k: u16, true_value: u64, num_of_unique: u64) -> Self {
        Self {
            qsk: KllSketch::new(k).unwrap(),
            sum_lb3: 0.0,
            sum_lb2: 0.0,
            sum_lb1: 0.0,
            sum_ub1: 0.0,
            sum_ub2: 0.0,
            sum_ub3: 0.0,
            sum_est: 0.0,
            sum_rel_err: 0.0,
            sum_sq_err: 0.0,
            true_value,
            num_of_unique,
        }
    }

    pub fn update(&mut self, result: &impl Estimator) {
        let est = result.estimate();
        self.qsk.update(KllFloat::<f64>::new(est).unwrap());
        self.sum_lb3 += result.lower_bound(NumStdDev::Three);
        self.sum_lb2 += result.lower_bound(NumStdDev::Two);
        self.sum_lb1 += result.lower_bound(NumStdDev::One);
        self.sum_ub1 += result.upper_bound(NumStdDev::One);
        self.sum_ub2 += result.upper_bound(NumStdDev::Two);
        self.sum_ub3 += result.upper_bound(NumStdDev::Three);
        self.sum_est += est;
        self.sum_rel_err += est / self.true_value as f64 - 1.0;
        let err = est - self.true_value as f64;
        self.sum_sq_err += err * err;
    }
}

/// One stats point per x-axis value `2^lg_min ..= 2^lg_max`.
pub fn build_log2_accuracy_stats(lg_min: u32, lg_max: u32, ppo: u32, lg_qk: u32) -> Vec<AccuracyStats> {
    let mut p = 1u64 << lg_min;
    (0..count_points(lg_min, lg_max, ppo))
        .map(|_| {
            let stats = AccuracyStats::new(1 << lg_qk, p, p);
            p = pwr2_series_next(ppo, p);
            stats
        })
        .collect()
}

/// Stats points with a fixed true value (the small set) and a growing large set.
pub fn build_log2_intersection_accuracy_stats(
    lg_min: u32,
    lg_max: u32,
    ppo: u32,
    lg_qk: u32,
) -> Vec<AccuracyStats> {
    let true_value = 1u64 << lg_min;
    let mut p = true_value;
    (0..count_points(lg_min, lg_max, ppo))
        .map(|_| {
            let stats = AccuracyStats::new(1 << lg_qk, true_value, p);
            p = pwr2_series_next(ppo, p);
            stats
        })
        .collect()
}

const HEADER: &str = "TrueU\tMeanEst\tMeanRelErr\tRMS_RE\tTrials\tMin\tQ(.00135)\tQ(.02275)\tQ(.15866)\tQ(.5)\tQ(.84134)\tQ(.97725)\tQ(.99865)\tMax\tavgLB3\tavgLB2\tavgLB1\tavgUB1\tavgUB2\tavgUB3";

/// Formats the accuracy table; `large_u` adds the intersection profile's leading column.
pub fn format_table(stats: &[AccuracyStats], cum_trials: usize, large_u: bool) -> String {
    let mut out = String::new();
    if large_u {
        out.push_str("LargeU\t");
    }
    out.push_str(HEADER);
    out.push('\n');
    let trials = cum_trials as f64;
    for q in stats {
        let true_u = q.true_value as f64;
        let rms_rel_err = (q.sum_sq_err / trials / (true_u * true_u)).sqrt();
        if large_u {
            write!(out, "{}\t", q.num_of_unique).unwrap();
        }
        write!(
            out,
            "{}\t{:.6e}\t{:.6e}\t{:.6e}\t{}",
            q.true_value,
            q.sum_est / trials,
            q.sum_rel_err / trials,
            rms_rel_err,
            cum_trials
        )
        .unwrap();
        let quantiles = q
            .qsk
            .quantiles(&GAUSSIANS_3SD, SearchCriteria::Inclusive)
            .unwrap()
            .unwrap();
        for quantile in quantiles {
            write!(out, "\t{:.6e}", quantile.into_inner() / true_u - 1.0).unwrap();
        }
        for sum in [q.sum_lb3, q.sum_lb2, q.sum_lb1, q.sum_ub1, q.sum_ub2, q.sum_ub3] {
            write!(out, "\t{:.6e}", sum / trials / true_u - 1.0).unwrap();
        }
        out.push('\n');
    }
    out
}

/// Runs trials in `tppo` steps, printing intermediate tables when `inter_data` is set.
pub fn run_accuracy(
    lg_min_t: u32,
    lg_max_t: u32,
    tppo: u32,
    inter_data: bool,
    max_u: u64,
    stats: &mut [AccuracyStats],
    large_u: bool,
    config: &str,
    mut run_trial: impl FnMut(&mut [AccuracyStats], u64) -> u64,
) {
    let start = Instant::now();
    let min_t = 1usize << lg_min_t;
    let max_t = 1usize << lg_max_t;
    let mut v_in = 0u64;
    let mut last_t = 0usize;
    while last_t < max_t {
        let next_t = if last_t == 0 {
            min_t
        } else {
            pwr2_series_next(tppo, last_t as u64) as usize
        };
        for _ in last_t..next_t {
            v_in = run_trial(stats, v_in);
        }
        last_t = next_t;
        if next_t >= max_t || inter_data {
            println!("{}", format_table(stats, last_t, large_u));
        }
        let cum_ms = start.elapsed().as_secs_f64() * 1e3;
        let per_trial_ms = cum_ms / last_t as f64;
        println!("Config:             : {config}");
        println!("Cum Trials          : {last_t}");
        println!("Cum Updates         : {v_in}");
        println!("Cum Time            : {:.3}s", cum_ms / 1e3);
        println!("Time Per Trial, mSec: {per_trial_ms:.6}");
        println!("Avg Update Time, nSec: {:.6}", per_trial_ms * 1e6 / max_u as f64);
        println!(
            "Est Time to Complete: {:.3}s",
            per_trial_ms * (max_t - last_t) as f64 / 1e3
        );
        println!();
    }
}
