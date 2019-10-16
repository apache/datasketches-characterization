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
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.hll.HyperLogLogHash;

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

  @Override
  public void shutdown() { }

  @Override
  public void cleanup() { }

  private void runMergeTrials() {
    long key = random.nextLong();

    final int numTrials = Integer.parseInt(job.getProperties().mustGet("numTrials"));
    final int numSketches = Integer.parseInt(job.getProperties().mustGet("numSketches"));
    final int distinctKeysPerSketch = Integer.parseInt(job.getProperties().mustGet("distinctKeysPerSketch"));
    final double trueCount = numSketches * distinctKeysPerSketch;
    double sumEstimates = 0;
    double sumOfSquaredDeviationsFromTrueCount = 0;

    for (int t = 0; t < numTrials; t++) {
      final HyperLogLogCollector union = HyperLogLogCollector.makeLatestCollector();

      for (int s = 0; s < numSketches; s++) {
        final HyperLogLogCollector sketch = HyperLogLogCollector.makeLatestCollector();
        for (int k = 0; k < distinctKeysPerSketch; k++) {
          DruidHllAccuracyProfile.longToByteArray(key++, bytes);
          sketch.add(hash.hash(bytes));
        }
        union.fold(sketch);
      }
      final double estimatedCount = union.estimateCardinality();
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
