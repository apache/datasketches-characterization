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

package org.apache.datasketches.characterization.hll;

import java.util.Random;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;

public class HllMergeAccuracyProfile implements JobProfile {

  private static final Random random = new Random();

  private Job job;

  @Override
  public void start(final Job job) {
    this.job = job;
    runMergeTrials();
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
        final HllSketch sketch = new HllSketch(lgK, TgtHllType.HLL_8);
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
    final double meanRelativeError = meanEstimate / trueCount - 1;
    final double relativeStandardError
      = Math.sqrt(sumOfSquaredDeviationsFromTrueCount / numTrials) / trueCount;
    job.println("True count: " + trueCount);
    job.println("Mean estimate: " + meanEstimate);
    job.println("Mean Relative Error: " + meanRelativeError);
    job.println("Relative Standard Error: " + relativeStandardError);
  }

}
