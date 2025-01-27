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

package org.apache.datasketches.characterization.tdigest;

import java.util.Random;

import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.quantiles.BaseQuantilesSpeedProfile;

import com.tdunning.math.stats.TDigest;

public class TDigestSpeedProfile extends BaseQuantilesSpeedProfile {

  private static final Random rnd = new Random();
  private int k;
  private double[] inputValues;
  private int numQueryValues;
  private double[] rankQueryValues;
  private double[] quantileQueryValues;

  long buildTimeNs;
  long updateTimeNs;
  long getRankTimeNs;
  long getQuantileTimeNs;
  long serializeTimeNs;
  long deserializeTimeNs;
  long fullSerializedSizeBytes;
  long smallSerializedSizeBytes;
  int numCentroids;

  @Override
  public void configure(final int k, final int numQueryValues, final Properties properties) {
    this.k = k;
    this.numQueryValues = numQueryValues;
  }

  @Override
  public void prepareTrial(final int streamLength) {
    // prepare input data
    inputValues = new double[streamLength];
    for (int i = 0; i < streamLength; i++) {
      inputValues[i] = rnd.nextDouble();
    }
    // prepare query data
    quantileQueryValues = new double[numQueryValues];
    for (int i = 0; i < numQueryValues; i++) {
      quantileQueryValues[i] = rnd.nextDouble();
    }
    rankQueryValues = new double[numQueryValues];
    for (int i = 0; i < numQueryValues; i++) {
      rankQueryValues[i] = rnd.nextDouble();
    }
    resetStats();
  }

  @Override
  public void doTrial() {
    final long startBuild = System.nanoTime();
    final TDigest sketch = TDigest.createDigest(k);
    final long stopBuild = System.nanoTime();
    buildTimeNs += stopBuild - startBuild;

    final long startUpdate = System.nanoTime();
    for (int i = 0; i < inputValues.length; i++) {
      sketch.add(inputValues[i]);
    }
    final long stopUpdate = System.nanoTime();
    updateTimeNs += stopUpdate - startUpdate;

    final long startGetRank = System.nanoTime();
    for (final double value: rankQueryValues) {
      sketch.cdf(value);
    }
    final long stopGetRank = System.nanoTime();
    getRankTimeNs += stopGetRank - startGetRank;

    final long startGetQuantile = System.nanoTime();
    for (final double value: quantileQueryValues) {
      sketch.quantile(value);
    }
    final long stopGetQuantile = System.nanoTime();
    getQuantileTimeNs += stopGetQuantile - startGetQuantile;

//    final long startSerialize = System.nanoTime();
//    final byte[] bytes = sketch.asBytes(null);
//    final long stopSerialize = System.nanoTime();
//    serializeTimeNs += stopSerialize - startSerialize;

//    final long startDeserialize = System.nanoTime();
//    final deserializedSketch = MergingDigest.fromBytes(bytes);
//    final long stopDeserialize = System.nanoTime();
//    deserializeTimeNs += stopDeserialize - startDeserialize;

    numCentroids += sketch.centroidCount();
    fullSerializedSizeBytes += sketch.byteSize();
    smallSerializedSizeBytes += sketch.smallByteSize();
  }

  @Override
  public String getHeader() {
    return "Stream\tTrials\tBuild\tUpdate\tRank\tQuant\tSer\tDeser\tCentroids\tFullSize\tSmallSize";
  }

  @Override
  public String getStats(final int streamLength, final int numTrials, final int numQueryValues) {
    return String.format("%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%d\t%d\t%d",
      streamLength,
      numTrials,
      (double) buildTimeNs / numTrials,
      (double) updateTimeNs / numTrials / streamLength,
      (double) getRankTimeNs / numTrials / numQueryValues,
      (double) getQuantileTimeNs / numTrials / numQueryValues,
      (double) serializeTimeNs / numTrials,
      (double) deserializeTimeNs / numTrials,
      numCentroids / numTrials,
      fullSerializedSizeBytes / numTrials,
      smallSerializedSizeBytes / numTrials
    );
  }

  private void resetStats() {
    buildTimeNs = 0;
    updateTimeNs = 0;
    getRankTimeNs = 0;
    getQuantileTimeNs = 0;
    serializeTimeNs = 0;
    deserializeTimeNs = 0;
    fullSerializedSizeBytes = 0;
    smallSerializedSizeBytes = 0;
    numCentroids = 0;
  }

}
