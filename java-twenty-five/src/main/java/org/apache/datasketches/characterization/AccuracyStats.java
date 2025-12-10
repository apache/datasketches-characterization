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
import static org.apache.datasketches.common.Util.powerSeriesNextDouble;
import static org.apache.datasketches.common.Util.pwr2SeriesNext;

import org.apache.datasketches.MonotonicPoints;
import org.apache.datasketches.quantiles.QuantilesDoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdatableQuantilesDoublesSketch;

/**
 * Holds key metrics from a set of accuracy trials
 *
 * @author Lee Rhodes
 */
public class AccuracyStats {
  public UpdatableQuantilesDoublesSketch qsk; //quantile sketch created by constructor
  public double sumEst = 0;
  public double sumRelErr = 0;
  public double sumSqErr = 0;
  public double rmsre = 0; //used later for plotting, set externally
  public double trueValue; //set by constructor, used only for error analysis
  public long uniques;     //set by constructor, used as a coordinate for intersection
  public int bytes = 0;

  /**
   * Used for single sketch or union accuracy.
   * @param k the configuration value for the quantiles sketch. It must be a power of two.
   * It is used to store the estimated rank values over a number of trials.
   * @param trueValue the true value
   */
  public AccuracyStats(final int k, final long trueValue) {
    qsk = new QuantilesDoublesSketchBuilder().setK(k).build();
    this.trueValue = trueValue;
    this.uniques = trueValue;
  }

  /**
   * Used for intersection accuracy
   * @param k the configuration value for the quantiles sketch. It must be a power of two.
   * @param trueValue the true value
   * @param uniques number of uniques, used as a coordinate in intersection testing.
   */
  public AccuracyStats(final int k, final long trueValue, final long uniques) {
    qsk = new QuantilesDoublesSketchBuilder().setK(k).build(); //Quantiles
    this.trueValue = trueValue;
    this.uniques = uniques;
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
   * Build the AccuracyStats Array based on fractional powers of 2
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
    long p = 1L << lgMin;
    for (int i = 0; i < qLen; i++) {
      qArr[i] = new AccuracyStats(1 << lgQK, p);
      p = pwr2SeriesNext(ppo, p);
    }
    return qArr;
  }

  /**
   * Build the AccuracyStats Array for Intersection based on fractional powers of 2.
   * All elements of the AccuracyStats array have 2^lgMin values as the trueValue.
   * @param lgMin log_base2 of the minimum number of uniques used
   * @param lgMax log_base2 of the maximum number of uniques used
   * @param ppo the number of points per octave
   * @param lgQK the lgK for the Quantiles sketch
   * @return an AccuracyStats array
   */
  public static final AccuracyStats[] buildLog2IntersectAccuracyStatsArray(
      final int lgMin, final int lgMax, final int ppo, final int lgQK) {
    final int qLen = MonotonicPoints.countPoints(lgMin, lgMax, ppo);
    final AccuracyStats[] qArr = new AccuracyStats[qLen];
    final long trueValue = 1L << lgMin;
    long p = trueValue; //becomes the uniques coordinate
    for (int i = 0; i < qLen; i++) {
      qArr[i] = new AccuracyStats(1 << lgQK, trueValue, p);
      p = pwr2SeriesNext(ppo, p);
    }
    return qArr;
  }

  /**
   * Build the AccuracyStats Array based on fractional powers of 10
   * @param log10Min log_base10 of the minimum number of uniques used
   * @param log10Max log_base10 of the maximum number of uniques used
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
      p = (long) powerSeriesNextDouble(ppb, p, true, 10.0);
    }
    return qArr;
  }

}
