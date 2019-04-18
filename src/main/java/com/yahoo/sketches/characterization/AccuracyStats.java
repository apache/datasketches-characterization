/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization;

import static com.yahoo.sketches.Util.pwr2LawNext;

import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

/**
 * Holds key metrics from a single accuracy trial
 *
 * @author Lee Rhodes
 */
public class AccuracyStats {
  public UpdateDoublesSketch qsk;
  public double sumEst = 0;
  public double sumRelErr = 0;
  public double sumSqErr = 0;
  public double rmsre = 0;
  public double trueValue;
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
   * @param est the value of the estimate for this trial
   * nanoSeconds.
   */
  public void update(final double est) {
    qsk.update(est);
    sumEst += est;
    sumRelErr += (est / trueValue) - 1.0;
    final double error = est - trueValue;
    sumSqErr += error * error;
  }

  /**
   * Build the Accuracy Stats Array
   * @param lgMin log_base2 of the minimum number of uniques used
   * @param lgMax log_base2 of the maximum number of uniques used
   * @param ppo the number of points per octave
   * @param lgQK the lgK for the Quantiles sketch
   * @return an AccuracyStats array
   */
  public static final AccuracyStats[] buildAccuracyStatsArray(
      final int lgMin, final int lgMax, final int ppo, final int lgQK) {
    final int qLen = PerformanceUtil.countPoints(lgMin, lgMax, ppo);
    final AccuracyStats[] qArr = new AccuracyStats[qLen];
    int p = 1 << lgMin;
    for (int i = 0; i < qLen; i++) {
      qArr[i] = new AccuracyStats(1 << lgQK, p);
      p = pwr2LawNext(ppo, p);
    }
    return qArr;
  }

}
