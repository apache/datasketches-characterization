/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.characterization;

import static org.apache.datasketches.Util.pwr2LawNext;

import org.apache.datasketches.MonotonicPoints;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

/**
 * Holds key metrics from a set of accuracy trials
 *
 * @author Lee Rhodes
 */
public class AccuracyStats {
  public UpdateDoublesSketch qsk; //quantile sketch created by constructor
  public double sumEst = 0;
  public double sumRelErr = 0;
  public double sumSqErr = 0;
  public double rmsre = 0; //used later for plotting
  public double trueValue; //set by constructor
  public int bytes = 0;

  /**
   * @param k the configuration value for the quantiles sketch. It must be a power of two.
   * @param trueValue the true value
   */
  public AccuracyStats(final int k, final int trueValue) {
    qsk = new DoublesSketchBuilder().setK(k).build(); //Quantiles
    this.trueValue = trueValue;
  }

  /**
   * Update
   *
   * @param est the value of the estimate for a single trial
   */
  public void update(final double est) {
    qsk.update(est);
    sumEst += est;
    sumRelErr += est / trueValue - 1.0;
    final double error = est - trueValue;
    sumSqErr += error * error;
  }

  /**
   * Build the AccuracyStats Array
   * @param lgMin log_base2 of the minimum number of uniques used
   * @param lgMax log_base2 of the maximum number of uniques used
   * @param ppo the number of points per octave
   * @param lgQK the lgK for the Quantiles sketch
   * @return an AccuracyStats array
   */
  public static final AccuracyStats[] buildAccuracyStatsArray(
      final int lgMin, final int lgMax, final int ppo, final int lgQK) {
    final int qLen = MonotonicPoints.countPoints(lgMin, lgMax, ppo);
    final AccuracyStats[] qArr = new AccuracyStats[qLen];
    int p = 1 << lgMin;
    for (int i = 0; i < qLen; i++) {
      qArr[i] = new AccuracyStats(1 << lgQK, p);
      p = pwr2LawNext(ppo, p);
    }
    return qArr;
  }

}
