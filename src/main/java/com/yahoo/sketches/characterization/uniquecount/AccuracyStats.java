/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

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
}
