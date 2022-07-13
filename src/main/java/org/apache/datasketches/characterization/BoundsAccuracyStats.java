/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.characterization;

import static org.apache.datasketches.Util.pwr2SeriesNext;

import org.apache.datasketches.MonotonicPoints;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

/**
 * Holds key metrics from a set of accuracy trials
 * @author Lee Rhodes
 */
public class BoundsAccuracyStats {
  public UpdateDoublesSketch qskEst; //quantile sketch created by constructor
  public double sumLB3 = 0;
  public double sumLB2 = 0;
  public double sumLB1 = 0;
  public double sumUB1 = 0;
  public double sumUB2 = 0;
  public double sumUB3 = 0;
  public double trueValue; //set by constructor

  public BoundsAccuracyStats(final int k, final int trueValue) {
    qskEst = new DoublesSketchBuilder().setK(k).build(); //Quantiles of estimates
    this.trueValue = trueValue;
  }

  /**
   * Update
   *
   * @param est the value of the estimate for a single trial
   * @param lb3 lower bound1
   * @param lb2 lower bound2
   * @param lb1 lower bound3
   * @param ub1 upper bound1
   * @param ub2 upper bound2
   * @param ub3 upper bound3
   */
  public void update(
      final double est,
      final double lb3,
      final double lb2,
      final double lb1,
      final double ub1,
      final double ub2,
      final double ub3) {
    qskEst.update(est);
    sumLB3 += lb3;
    sumLB2 += lb2;
    sumLB1 += lb1;
    sumUB1 += ub1;
    sumUB2 += ub2;
    sumUB3 += ub3;
  }

  /**
   * Build the BoundsAccuracyStats Array
   * @param lgMin log_base2 of the minimum number of uniques used
   * @param lgMax log_base2 of the maximum number of uniques used
   * @param ppo the number of points per octave
   * @param lgQK the lgK for the Quantiles sketch
   * @return a BoundsAccuracyStats array
   */
  public static final BoundsAccuracyStats[] buildAccuracyStatsArray(
      final int lgMin, final int lgMax, final int ppo, final int lgQK) {
    final int qLen = MonotonicPoints.countPoints(lgMin, lgMax, ppo);
    final BoundsAccuracyStats[] qArr = new BoundsAccuracyStats[qLen];
    int p = 1 << lgMin;
    for (int i = 0; i < qLen; i++) {
      qArr[i] = new BoundsAccuracyStats(1 << lgQK, p);
      p = (int)pwr2SeriesNext(ppo, p);
    }
    return qArr;
  }
}
