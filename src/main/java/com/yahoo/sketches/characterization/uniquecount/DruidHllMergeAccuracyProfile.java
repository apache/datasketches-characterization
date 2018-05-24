/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

import java.util.Random;

import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.characterization.uniquecount.druidhll.HyperLogLogCollector;
import com.yahoo.sketches.characterization.uniquecount.druidhll.HyperLogLogHash;

public class DruidHllMergeAccuracyProfile implements JobProfile {

  private static final Random random = new Random();

  private static final HyperLogLogHash hash = HyperLogLogHash.getDefault();
  private static final byte[] bytes = new byte[8]; // for key conversion

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

  private void runMergeTrials() {
    long key = random.nextLong();

    final int numTrials = Integer.parseInt(job.getProperties().mustGet("numTrials"));
    final int numSketches = Integer.parseInt(job.getProperties().mustGet("numSketches"));
    final int distinctKeysPerSketch = Integer.parseInt(job.getProperties().mustGet("distinctKeysPerSketch"));
    final double trueCount = numSketches * distinctKeysPerSketch;
    double sumEstimates = 0;
    double sumErrors = 0;

    for (int t = 0; t < numTrials; t++) {
      HyperLogLogCollector union = HyperLogLogCollector.makeLatestCollector();

      for (int s = 0; s < numSketches; s++) {
        HyperLogLogCollector sketch = HyperLogLogCollector.makeLatestCollector();
        for (int k = 0; k < distinctKeysPerSketch; k++) {
          DruidHllAccuracyProfile.longToByteArray(key++, bytes);
          sketch.add(hash.hash(bytes));
        }
        union.fold(sketch);
      }
      final double estimatedCount = union.estimateCardinality();
      sumEstimates += estimatedCount;
      final double relativeError = Math.abs(trueCount - estimatedCount) / trueCount;
      sumErrors += relativeError;
    }
    println("True count: " + trueCount);
    println("Mean estimate: " + sumEstimates / numTrials);
    println("Mean Relative Error: " + sumErrors / numTrials);
  }

}
