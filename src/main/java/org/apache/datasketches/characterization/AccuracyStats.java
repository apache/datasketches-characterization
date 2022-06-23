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

import static java.lang.Math.pow;
import static java.lang.Math.round;
import static org.apache.datasketches.Util.powerSeriesNextDouble;
import static org.apache.datasketches.Util.pwr2SeriesNext;

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
  public double rmsre = 0; //used later for plotting, set externally
  public double trueValue; //set by constructor
  public int bytes = 0;

  /**
   * @param k the configuration value for the quantiles sketch. It must be a power of two.
   * @param trueValue the true value
   */
  public AccuracyStats(final int k, final long trueValue) {
    qsk = new DoublesSketchBuilder().setK(k).build(); //Quantiles
    this.trueValue = trueValue;
  }

  /**
   * Update
   *
   * @param est the value of the estimate for a single measurement
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
  public static final AccuracyStats[] buildLog2AccuracyStatsArray(
      final int lgMin, final int lgMax, final int ppo, final int lgQK) {
    final int qLen = MonotonicPoints.countPoints(lgMin, lgMax, ppo);
    final AccuracyStats[] qArr = new AccuracyStats[qLen];
    int p = 1 << lgMin;
    for (int i = 0; i < qLen; i++) {
      qArr[i] = new AccuracyStats(1 << lgQK, p);
      p = pwr2SeriesNext(ppo, p);
    }
    return qArr;
  }

  /**
   * Build the AccuracyStats Array
   * @param log10Min log_base2 of the minimum number of uniques used
   * @param log10Max log_base2 of the maximum number of uniques used
   * @param ppb the number of points per base (10)
   * @param lgQK the lgK for the Quantiles sketch
   * @return an AccuracyStats array
   */
  public static final AccuracyStats[] buildLog10AccuracyStatsArray(
      final int log10Min, final int log10Max, final int ppb, final int lgQK) {
    final int qLen = MonotonicPoints.countLog10Points(log10Min, log10Max, ppb);
    final AccuracyStats[] qArr = new AccuracyStats[qLen];
    long p = round(pow(10, log10Min));
    for (int i = 0; i < qLen; i++) {
      qArr[i] = new AccuracyStats(1 << lgQK, p);
      p = (int) powerSeriesNextDouble(ppb, p, true, 10.0);
    }
    return qArr;
  }

}
