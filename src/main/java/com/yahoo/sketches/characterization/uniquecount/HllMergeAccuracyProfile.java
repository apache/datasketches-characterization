/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

import java.util.Random;

import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.Union;

public class HllMergeAccuracyProfile implements JobProfile {

  private static final Random random = new Random();

  private Job job;

  @Override
  public void start(final Job job) {
    this.job = job;
    runMergeTrials();
  }

  @Override
  public void println(final String s) {
    job.println(s);
  }

  @Override
  public void shutdown() { }

  @Override
  public void cleanup() { }

  private void runMergeTrials() {
    long key = random.nextLong();

    final int lgK = Integer.parseInt(job.getProperties().mustGet("lgK"));
    final int numTrials = Integer.parseInt(job.getProperties().mustGet("numTrials"));
    final int numSketches = Integer.parseInt(job.getProperties().mustGet("numSketches"));
    final int distinctKeysPerSketch = Integer.parseInt(job.getProperties().mustGet("distinctKeysPerSketch"));
    final double trueCount = numSketches * distinctKeysPerSketch;
    double sumEstimates = 0;
    double sumOfSquaredDeviationsFromTrueCount = 0;

    for (int t = 0; t < numTrials; t++) {
      final Union union = new Union(lgK);

      for (int s = 0; s < numSketches; s++) {
        final HllSketch sketch = new HllSketch(lgK);
        for (int k = 0; k < distinctKeysPerSketch; k++) {
          sketch.update(key++);
        }
        union.update(sketch);
      }
      final double estimatedCount = union.getResult().getEstimate();
      sumEstimates += estimatedCount;
      sumOfSquaredDeviationsFromTrueCount += (estimatedCount - trueCount) * (estimatedCount - trueCount);
    }
    final double meanEstimate = sumEstimates / numTrials;
    final double meanRelativeError = (meanEstimate / trueCount) - 1;
    final double relativeStandardError
      = Math.sqrt(sumOfSquaredDeviationsFromTrueCount / numTrials) / trueCount;
    println("True count: " + trueCount);
    println("Mean estimate: " + meanEstimate);
    println("Mean Relative Error: " + meanRelativeError);
    println("Relative Standard Error: " + relativeStandardError);
  }

}
