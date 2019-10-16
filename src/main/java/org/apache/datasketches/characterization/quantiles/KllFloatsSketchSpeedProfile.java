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

package org.apache.datasketches.characterization.quantiles;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.Properties;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;

public class KllFloatsSketchSpeedProfile extends BaseQuantilesSpeedProfile {

  private static final Random rnd = new Random();
  private int k;
  private float[] inputValues;
  private int numQueryValues;
  private float[] rankQueryValues;
  private double[] quantileQueryValues;

  long buildTimeNs;
  long updateTimeNs;
  long getQuantileTimeNs;
  long getQuantilesTimeNs;
  long getRankTimeNs;
  long getCdfTimeNs;
  long serializeTimeNs;
  long deserializeTimeNs;
  long numRetainedItems;
  long serializedSizeBytes;

  @Override
  void configure(final int k, final int numQueryValues, final Properties properties) {
    this.k = k;
    this.numQueryValues = numQueryValues;
  }

  @Override
  void prepareTrial(final int streamLength) {
    // prepare input data
    inputValues = new float[streamLength];
    for (int i = 0; i < streamLength; i++) {
      inputValues[i] = rnd.nextFloat();
    }
    // prepare query data that must be ordered
    quantileQueryValues = new double[numQueryValues];
    for (int i = 0; i < numQueryValues; i++) {
      quantileQueryValues[i] = rnd.nextDouble();
    }
    rankQueryValues = new float[numQueryValues];
    for (int i = 0; i < numQueryValues; i++) {
      rankQueryValues[i] = rnd.nextFloat();
    }
    Arrays.sort(rankQueryValues);
    resetStats();
  }

  @Override
  void doTrial() {
    final long startBuild = System.nanoTime();
    final KllFloatsSketch sketch = new KllFloatsSketch(k);
    final long stopBuild = System.nanoTime();
    buildTimeNs += stopBuild - startBuild;

    final long startUpdate = System.nanoTime();
    for (int i = 0; i < inputValues.length; i++) {
      sketch.update(inputValues[i]);
    }
    final long stopUpdate = System.nanoTime();
    updateTimeNs += stopUpdate - startUpdate;

    final long startGetQuantile = System.nanoTime();
    for (final double value: quantileQueryValues) {
      sketch.getQuantile(value);
    }
    final long stopGetQuantile = System.nanoTime();
    getQuantileTimeNs += stopGetQuantile - startGetQuantile;

    final long startGetQuantiles = System.nanoTime();
    sketch.getQuantiles(quantileQueryValues);
    final long stopGetQuantiles = System.nanoTime();
    getQuantilesTimeNs += stopGetQuantiles - startGetQuantiles;

    final long startGetRank = System.nanoTime();
    for (final float value: rankQueryValues) {
      sketch.getRank(value);
    }
    final long stopGetRank = System.nanoTime();
    getRankTimeNs += stopGetRank - startGetRank;

    final long startGetCdf = System.nanoTime();
    sketch.getCDF(rankQueryValues);
    final long stopGetCdf = System.nanoTime();
    getCdfTimeNs += stopGetCdf - startGetCdf;

    final long startSerialize = System.nanoTime();
    final byte[] bytes = sketch.toByteArray();
    final long stopSerialize = System.nanoTime();
    serializeTimeNs += stopSerialize - startSerialize;

    final Memory mem = Memory.wrap(bytes);
    final long startDeserialize = System.nanoTime();
    KllFloatsSketch.heapify(mem);
    final long stopDeserialize = System.nanoTime();
    deserializeTimeNs += stopDeserialize - startDeserialize;

    // could record the last one since they must be the same
    // but let's average across all trials to see if there is an anomaly
    numRetainedItems += sketch.getNumRetained();
    serializedSizeBytes += sketch.getSerializedSizeBytes();
  }

  @Override
  String getHeader() {
    return "Stream\tTrials\tBuild\tUpdate\tQuant\tQuants\tRank\tCDF\tSer\tDeser\tItems\tSize";
  }

  @Override
  String getStats(final int streamLength, final int numTrials, final int numQueryValues) {
    return String.format("%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%d\t%d",
      streamLength,
      numTrials,
      (double) buildTimeNs / numTrials,
      (double) updateTimeNs / numTrials / streamLength,
      (double) getQuantileTimeNs / numTrials / numQueryValues,
      (double) getQuantilesTimeNs / numTrials / numQueryValues,
      (double) getRankTimeNs / numTrials / numQueryValues,
      (double) getCdfTimeNs / numTrials / numQueryValues,
      (double) serializeTimeNs / numTrials,
      (double) deserializeTimeNs / numTrials,
      numRetainedItems / numTrials,
      serializedSizeBytes / numTrials
    );
  }

  private void resetStats() {
    buildTimeNs = 0;
    updateTimeNs = 0;
    getQuantileTimeNs = 0;
    getQuantilesTimeNs = 0;
    getRankTimeNs = 0;
    getCdfTimeNs = 0;
    serializeTimeNs = 0;
    deserializeTimeNs = 0;
    numRetainedItems = 0;
    serializedSizeBytes = 0;
  }

  static void shuffle(final float[] array) {
    for (int i = 0; i < array.length; i++) {
      final int r = rnd.nextInt(i + 1);
      swap(array, i, r);
    }
  }

  private static void swap(final float[] array, final int i1, final int i2) {
    final float value = array[i1];
    array[i1] = array[i2];
    array[i2] = value;
  }

}
