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

package org.apache.datasketches.characterization.hll;

import java.util.Random;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import com.google.zetasketch.HyperLogLogPlusPlus;

/**
 * @author Lee Rhodes
 */
public class ZetaHllMergeAccuracyProfile  implements JobProfile {
  private HyperLogLogPlusPlus.Builder hllBuilder = new HyperLogLogPlusPlus.Builder();
  private HyperLogLogPlusPlus<Long> target;

  private static final Random random = new Random();

  private Job job;
  private int lgK;
  private int numTrials;
  private int numSketches;
  private int distinctKeysPerSketch;

  @Override
  public void start(Job job) {
    this.job = job;
    lgK = Integer.parseInt(job.getProperties().mustGet("lgK"));
    numTrials = Integer.parseInt(job.getProperties().mustGet("numTrials"));
    numSketches = Integer.parseInt(job.getProperties().mustGet("numSketches"));
    distinctKeysPerSketch = Integer.parseInt(job.getProperties().mustGet("distinctKeysPerSketch"));
    runMergeTrials();
  }

  private HyperLogLogPlusPlus<Long> newSketch(final int lgK) {
    final int lgSP = Math.min(lgK + 5, 25);
    hllBuilder.normalPrecision(lgK);
    hllBuilder.sparsePrecision(lgSP);
    return hllBuilder.buildForLongs();
  }

  @Override
  public void shutdown() { }

  @Override
  public void cleanup() { }

  @Override
  public void println(Object obj) {
    job.println(obj);
  }

  private void runMergeTrials() {
    long key = random.nextLong();

    final double trueCount = numSketches * distinctKeysPerSketch;
    double sumEstimates = 0;
    double sumOfSquaredDeviationsFromTrueCount = 0;

    for (int t = 0; t < numTrials; t++) {
      target = newSketch(lgK);

      for (int s = 0; s < numSketches; s++) {
        final HyperLogLogPlusPlus<Long> sketch = newSketch(lgK);
        for (int k = 0; k < distinctKeysPerSketch; k++) {
          sketch.add(key++);
        }
        target.merge(sketch);
      }
      final double estimatedCount = target.result();
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
