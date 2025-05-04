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

package org.apache.datasketches.characterization.kll;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.quantiles.BaseQuantilesSpeedProfile;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;

@SuppressWarnings("unused")
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

  @Override //before all trials and before all streamLengths
  public void configure(final int k, final int numQueryValues, final Properties properties) {
    this.k = k;
    this.numQueryValues = numQueryValues;

    quantileQueryValues = new double[numQueryValues];
    final double incrd = 1.0 / numQueryValues;
    double inVd = 0;
    for (int i = 0; i < numQueryValues; i++) {
      quantileQueryValues[i] =  inVd += incrd; //rnd.nextDouble();
    }

    rankQueryValues = new float[numQueryValues];
    final float incrf2 = 1.0F / numQueryValues;
    float inVf2 = 0;
    for (int i = 0; i < numQueryValues; i++) {
      rankQueryValues[i] = inVf2 += incrf2; //rnd.nextFloat();
    }
  }

  @Override //prepare streamLength for all trials
  public void prepareTrial(final int streamLength) {
    // prepare input data
    inputValues = new float[streamLength];
    final float incrf = 1.0F / streamLength;
    float inVf = 0;
    for (int i = 0; i < streamLength; i++) {
      inputValues[i] = inVf += incrf; //rnd.nextFloat();
    }
    resetStats();
  }

  @Override
  public void doTrial() {
    final long startBuild = System.nanoTime();
    final KllFloatsSketch sketch =  KllFloatsSketch.newHeapInstance(k);
    final long stopBuild = System.nanoTime();
    buildTimeNs += stopBuild - startBuild;

    for (int i = 0; i < inputValues.length; i++) { sketch.update(inputValues[i]); }
    final long stopUpdate = System.nanoTime();
    updateTimeNs += stopUpdate - stopBuild;

//    for (final double r: quantileQueryValues) { sketch.getQuantile(r); }
//    final long stopGetQuantile = System.nanoTime();
//    getQuantileTimeNs += stopGetQuantile - stopUpdate;
//
//    sketch.getQuantiles(quantileQueryValues);
//    final long stopGetQuantiles = System.nanoTime();
//    getQuantilesTimeNs += stopGetQuantiles - stopGetQuantile;
//
//    for (final float value: rankQueryValues) { sketch.getRank(value); }
//    final long stopGetRank = System.nanoTime();
//    getRankTimeNs += stopGetRank - stopGetQuantiles;
//
//    sketch.getRanks(rankQueryValues);
//    final long stopGetCdf = System.nanoTime();
//    getCdfTimeNs += stopGetCdf - stopGetRank;

//    final byte[] bytes = sketch.toByteArray();
//    final long stopSerialize = System.nanoTime();
//    serializeTimeNs += stopSerialize - stopGetCdf;
//
//    final Memory mem = Memory.wrap(bytes);
//    KllFloatsSketch.heapify(mem);
//    final long stopDeserialize = System.nanoTime();
//    deserializeTimeNs += stopDeserialize - stopSerialize;

    // could record the last one since they must be the same
    // but let's average across all trials to see if there is an anomaly
//    numRetainedItems += sketch.getNumRetained();
//    serializedSizeBytes += sketch.getSerializedSizeBytes();
  }

  @Override
  public String getHeader() {
    return "StreamLen\tTrials\tBuild\tUpdate\tQuant\tQuants\tRank\tRanks\tSer\tDeser\tRetItems\tSerSize";
  }

  @Override
  public String getStats(final int streamLength, final int numTrials, final int numQueryValues) {
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
