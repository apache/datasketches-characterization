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

use std::collections::HashMap;
use std::str::FromStr;

pub trait JobProfile {
    fn run(&mut self);
}

/// Job configuration overrides given on the command line as `name=value`.
#[derive(Default)]
pub struct Overrides(HashMap<String, String>);

impl Overrides {
    pub fn parse(args: &[String]) -> Self {
        Self(
            args.iter()
                .map(|arg| {
                    let (key, value) = arg
                        .split_once('=')
                        .unwrap_or_else(|| panic!("expected name=value, got {arg}"));
                    (key.to_string(), value.to_string())
                })
                .collect(),
        )
    }

    pub fn get<T: FromStr>(&self, key: &str, default: T) -> T {
        self.0.get(key).map_or(default, |value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("invalid value for {key}: {value}"))
        })
    }
}

/// Returns the number of plotting points between `2^lg_start` and `2^lg_end` with `ppo` points
/// per octave.
pub fn count_points(lg_start: u32, lg_end: u32, ppo: u32) -> usize {
    let mut p = 1u64 << lg_start;
    let end = 1u64 << lg_end;
    let mut count = 0;
    while p <= end {
        p = pwr2_series_next(ppo, p);
        count += 1;
    }
    count
}

/// Returns the next point of a power-of-two series with `ppo` points per octave.
pub fn pwr2_series_next(ppo: u32, cur_point: u64) -> u64 {
    let cur = cur_point.max(1);
    let mut gi = ((cur as f64).log2() * f64::from(ppo)).round() as i64;
    loop {
        gi += 1;
        let next = 2f64.powf(gi as f64 / f64::from(ppo)).round() as u64;
        if next > cur_point {
            return next;
        }
    }
}

/// Returns the number of trials at `cur_u`, decreasing from `2^lg_max_t` at `2^lg_min_bpu` to
/// `2^lg_min_t` at `2^lg_max_bpu`.
pub fn num_trials(cur_u: u64, lg_min_t: u32, lg_max_t: u32, lg_min_bpu: u32, lg_max_bpu: u32) -> usize {
    let max_t = 1usize << lg_max_t;
    let min_t = 1usize << lg_min_t;
    if lg_min_t == lg_max_t || cur_u <= 1u64 << lg_min_bpu {
        return max_t;
    }
    if cur_u >= 1u64 << lg_max_bpu {
        return min_t;
    }
    let slope = (f64::from(lg_max_t) - f64::from(lg_min_t))
        / (f64::from(lg_min_bpu) - f64::from(lg_max_bpu));
    let lg_trials = slope * ((cur_u as f64).log2() - f64::from(lg_min_bpu)) + f64::from(lg_max_t);
    2f64.powf(lg_trials) as usize
}

/// Iterates the x-axis points `2^lg_min_u ..= 2^lg_max_u` with `uppo` points per octave.
pub fn u_points(lg_min_u: u32, lg_max_u: u32, uppo: u32) -> impl Iterator<Item = u64> {
    let max_u = 1u64 << lg_max_u;
    let mut last = 0u64;
    std::iter::from_fn(move || {
        if last >= max_u {
            return None;
        }
        last = if last == 0 {
            1u64 << lg_min_u
        } else {
            pwr2_series_next(uppo, last)
        };
        Some(last)
    })
}

/// Prints `header`, then one row per x-axis point; `trial` returns per-trial measurements that are
/// averaged over `trials(u)` trials and printed with `{:.6e}`.
pub fn run_speed<const N: usize>(
    header: &str,
    points: impl Iterator<Item = u64>,
    trials: impl Fn(u64) -> usize,
    mut trial: impl FnMut(u64) -> [f64; N],
) {
    println!("{header}");
    for u in points {
        let trials = trials(u);
        let mut sums = [0.0; N];
        for _ in 0..trials {
            for (sum, value) in sums.iter_mut().zip(trial(u)) {
                *sum += value;
            }
        }
        let mut row = format!("{u}\t{trials}");
        for sum in sums {
            row.push_str(&format!("\t{:.6e}", sum / trials as f64));
        }
        println!("{row}");
    }
}

/// Returns the nanoseconds taken by `f` and its result.
pub fn time_ns<T>(f: impl FnOnce() -> T) -> (f64, T) {
    let start = std::time::Instant::now();
    let result = std::hint::black_box(f());
    (start.elapsed().as_nanos() as f64, result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn series_matches_go() {
        let points: Vec<u64> = u_points(0, 5, 4).collect();
        assert_eq!(points, vec![1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 13, 16, 19, 23, 27, 32]);
        assert_eq!(count_points(0, 5, 4), 16);
    }

    #[test]
    fn trials_slope_down() {
        assert_eq!(num_trials(1, 4, 20, 4, 20), 1 << 20);
        assert_eq!(num_trials(1 << 20, 4, 20, 4, 20), 1 << 4);
        assert_eq!(num_trials(1 << 12, 4, 20, 4, 20), 1 << 12);
    }
}
