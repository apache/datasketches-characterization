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
import java.util.Comparator;
import java.util.Random;

import org.apache.datasketches.Properties;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantiles.ItemsSketch;

public class ItemsSketchSpeedProfile extends BaseQuantilesSpeedProfile {

  private static final Comparator<Double> COMPARATOR = Comparator.naturalOrder();
  private static final ArrayOfDoublesSerDe SERDE = new ArrayOfDoublesSerDe();
  private static final Random rnd = new Random();
  private int k;
  private Double[] randInput;
  private int numQueryValues;
  private Double[] orderedBigDoubles;
  private double[] orderedLittleDoubles;

  long buildTimeNs;
  long updateTimeNs;
  long getQuantilesTimeNs;
  long getCdfTimeNs;
  long getRankTimeNs;
  long serializeTimeNs;
  long deserializeTimeNs;
  long numRetainedItems;
  long serializedSizeBytes;

  @Override
  public void configure(final int k, final int numQueryValues, final Properties properties) {
    this.k = k;
    this.numQueryValues = numQueryValues;
  }

  @Override
  public void prepareTrial(final int streamLength) {
    // prepare input data
    randInput = new Double[streamLength]; //random input doubles [0,1]
    for (int i = 0; i < streamLength; i++) {
      randInput[i] = rnd.nextDouble();
    }
    // prepare query data that must be ordered
    orderedLittleDoubles = new double[numQueryValues];
    orderedBigDoubles = new Double[numQueryValues];
    for (int i = 0; i < numQueryValues; i++) { //create the little d's
      orderedLittleDoubles[i] = rnd.nextDouble();
    }
    Arrays.sort(orderedLittleDoubles); //sort the little d's
    for (int i = 0; i < numQueryValues; i++) { //copy to the big D's
      orderedBigDoubles[i] = orderedLittleDoubles[i];
    }
    resetStats();
  }

  @SuppressWarnings("unused")
  @Override
  public void doTrial() {
    shuffle(randInput);

    final long startBuild = System.nanoTime();
    final ItemsSketch<Double> sketch = ItemsSketch.getInstance(Double.class, k, COMPARATOR);
    final long stopBuild = System.nanoTime();
    buildTimeNs += stopBuild - startBuild;

    final long startUpdate = System.nanoTime();
    for (int i = 0; i < randInput.length; i++) {
      sketch.update(randInput[i]);
    }
    final long stopUpdate = System.nanoTime();
    updateTimeNs += stopUpdate - startUpdate;

    final long startGetQuantiles = System.nanoTime();
    sketch.getQuantiles(orderedLittleDoubles);
    final long stopGetQuantiles = System.nanoTime();
    getQuantilesTimeNs += stopGetQuantiles - startGetQuantiles;

    final long startGetCdf = System.nanoTime();
    sketch.getCDF(orderedBigDoubles);
    final long stopGetCdf = System.nanoTime();
    getCdfTimeNs += stopGetCdf - startGetCdf;

    final long startGetRanks = System.nanoTime();
    final double[] estRanks = sketch.getRanks(orderedBigDoubles);
    final long stopGetRank = System.nanoTime();
    getRankTimeNs += stopGetRank - startGetRanks;

    final long startSerialize = System.nanoTime();
    final byte[] bytes = sketch.toByteArray(SERDE);
    final long stopSerialize = System.nanoTime();
    serializeTimeNs += stopSerialize - startSerialize;

    final WritableMemory mem = WritableMemory.writableWrap(bytes);
    final long startDeserialize = System.nanoTime();
    ItemsSketch.getInstance(Double.class, mem, COMPARATOR, SERDE);
    final long stopDeserialize = System.nanoTime();
    deserializeTimeNs += stopDeserialize - startDeserialize;

    // could record the last one since they must be the same
    // but let's average across all trials to see if there is an anomaly
    numRetainedItems += sketch.getNumRetained();
    serializedSizeBytes += bytes.length;
  }

  @Override
  public String getHeader() {
    return "Stream\tTrials\tBuild\tUpdate\tQuant\tCDF\tRank\tSer\tDeser\tItems\tstatsSize";
  }

  @Override
  public String getStats(final int streamLength, final int numTrials, final int numQueryValues) {
    return String.format("%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%d\t%d",
      streamLength,
      numTrials,
      (double) buildTimeNs / numTrials,
      (double) updateTimeNs / numTrials / streamLength,
      (double) getQuantilesTimeNs / numTrials / numQueryValues,
      (double) getCdfTimeNs / numTrials / numQueryValues,
      (double) getRankTimeNs / numTrials / numQueryValues,
      (double) serializeTimeNs / numTrials,
      (double) deserializeTimeNs / numTrials,
      numRetainedItems / numTrials,
      serializedSizeBytes / numTrials
    );
  }

  private void resetStats() {
    buildTimeNs = 0;
    updateTimeNs = 0;
    getQuantilesTimeNs = 0;
    getCdfTimeNs = 0;
    getRankTimeNs = 0;
    serializeTimeNs = 0;
    deserializeTimeNs = 0;
    numRetainedItems = 0;
    serializedSizeBytes = 0;
  }

  static void shuffle(final Double[] array) {
    for (int i = 0; i < array.length; i++) {
      final int r = rnd.nextInt(i + 1);
      swap(array, i, r);
    }
  }

  private static void swap(final Double[] array, final int i1, final int i2) {
    final Double value = array[i1];
    array[i1] = array[i2];
    array[i2] = value;
  }
}
