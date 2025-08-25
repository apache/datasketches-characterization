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
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

public class DoublesSketchSpeedProfile extends BaseQuantilesSpeedProfile {

  private static final Random rnd = new Random();
  private int k;
  private DoublesSketchBuilder builder;
  private double[] randomInput;
  private int numQueryValues;
  private double[] orderedLittleDoubles;
  private boolean useDirect;
  private WritableMemory updateSketchMemory;
  private WritableMemory compactSketchMemory;

  long buildTimeNs;
  long updateTimeNs;
  long updateGetQuantilesTimeNs;
  long updateGetCdfTimeNs;
  long updateGetRankTimeNs;
  long updateSerializeTimeNs;
  long updateDeserializeTimeNs;
  long updateSerializedSizeBytes;
  long compactTimeNs;
  long compactGetQuantilesTimeNs;
  long compactGetCdfTimeNs;
  long compactGetRankTimeNs;
  long compactSerializeTimeNs;
  long compactDeserializeTimeNs;
  long compactSerializedSizeBytes;
  long numRetainedItems;

  @Override
  public void configure(final int k, final int numQueryValues, final Properties properties) {
    this.k = k;
    this.numQueryValues = numQueryValues;
    useDirect = Boolean.parseBoolean(properties.mustGet("useDirect"));
    builder = DoublesSketch.builder().setK(k);
  }

  @Override
  public void prepareTrial(final int streamLength) {
    // prepare input data
    randomInput = new double[streamLength];
    for (int i = 0; i < streamLength; i++) {
      randomInput[i] = rnd.nextDouble();
    }
    // prepare query data that must be ordered
    orderedLittleDoubles = new double[numQueryValues];
    for (int i = 0; i < numQueryValues; i++) {
      orderedLittleDoubles[i] = rnd.nextDouble();
    }
    Arrays.sort(orderedLittleDoubles);
    if (useDirect) {
      updateSketchMemory = WritableMemory
          .writableWrap(new byte[DoublesSketch.getUpdatableStorageBytes(k, streamLength)]);
      compactSketchMemory = WritableMemory
          .writableWrap(new byte[DoublesSketch.getCompactSerialiedSizeBytes(k, streamLength)]);
    }
    resetStats();
  }

  @SuppressWarnings("unused")
  @Override
  public void doTrial() {
    DoublesSketchAccuracyProfile.shuffle(randomInput);

    final long startBuild = System.nanoTime();
    final UpdateDoublesSketch updateSketch = useDirect
        ? builder.build(updateSketchMemory)
        : builder.build();
    final long stopBuild = System.nanoTime();
    buildTimeNs += stopBuild - startBuild;

    final long startUpdate = System.nanoTime();
    for (int i = 0; i < randomInput.length; i++) {
      updateSketch.update(randomInput[i]);
    }
    final long stopUpdate = System.nanoTime();
    updateTimeNs += stopUpdate - startUpdate;

    {
      final long startGetQuantiles = System.nanoTime();
      updateSketch.getQuantiles(orderedLittleDoubles);
      final long stopGetQuantiles = System.nanoTime();
      updateGetQuantilesTimeNs += stopGetQuantiles - startGetQuantiles;

      final long startGetCdf = System.nanoTime();
      updateSketch.getCDF(orderedLittleDoubles);
      final long stopGetCdf = System.nanoTime();
      updateGetCdfTimeNs += stopGetCdf - startGetCdf;

      final long startGetRank = System.nanoTime();
      final double[] estRanks = updateSketch.getRanks(orderedLittleDoubles);
      final long stopGetRank = System.nanoTime();
      updateGetRankTimeNs += stopGetRank - startGetRank;

      final long startSerialize = System.nanoTime();
      final byte[] bytes = updateSketch.toByteArray();
      final long stopSerialize = System.nanoTime();
      updateSerializeTimeNs += stopSerialize - startSerialize;

      final WritableMemory mem = WritableMemory.writableWrap(bytes);
      final long startDeserialize = System.nanoTime();
      if (useDirect) {
        UpdateDoublesSketch.wrap(mem);
      } else {
        UpdateDoublesSketch.heapify(mem);
      }
      final long stopDeserialize = System.nanoTime();
      updateDeserializeTimeNs += stopDeserialize - startDeserialize;

      // could record the last one since they must be the same
      // but let's average across all trials to see if there is an anomaly
      updateSerializedSizeBytes += bytes.length;
    }

    final long startCompact = System.nanoTime();
    final DoublesSketch compactSketch = useDirect
        ? updateSketch.compact(compactSketchMemory)
        : updateSketch.compact();
    final long stopCompact = System.nanoTime();
    compactTimeNs += stopCompact - startCompact;

    {
      final long startGetQuantiles = System.nanoTime();
      compactSketch.getQuantiles(orderedLittleDoubles);
      final long stopGetQuantiles = System.nanoTime();
      compactGetQuantilesTimeNs += stopGetQuantiles - startGetQuantiles;

      final long startGetCdf = System.nanoTime();
      compactSketch.getCDF(orderedLittleDoubles);
      final long stopGetCdf = System.nanoTime();
      compactGetCdfTimeNs += stopGetCdf - startGetCdf;

      final long startGetRank = System.nanoTime();
      for (final double value: orderedLittleDoubles) {
        //compactSketch.getRank(value);
        final double estRank = compactSketch.getCDF(new double[] {value})[0];
      }
      final long stopGetRank = System.nanoTime();
      compactGetRankTimeNs += stopGetRank - startGetRank;

      final long startSerialize = System.nanoTime();
      final byte[] bytes = compactSketch.toByteArray();
      final long stopSerialize = System.nanoTime();
      compactSerializeTimeNs += stopSerialize - startSerialize;

      final Memory mem = Memory.wrap(bytes);
      final long startDeserialize = System.nanoTime();
      if (useDirect) {
        DoublesSketch.wrap(mem);
      } else {
        DoublesSketch.heapify(mem);
      }
      final long stopDeserialize = System.nanoTime();
      compactDeserializeTimeNs += stopDeserialize - startDeserialize;

      // could record the last one since they must be the same
      // but let's average across all trials to see if there is an anomaly
      compactSerializedSizeBytes += bytes.length;
      numRetainedItems += compactSketch.getNumRetained();
    }
  }

  @Override
  public String getHeader() {
    return "Stream\tTrials\tBuild\tUpdate\tQuant\tCDF\tRank\tSer\tDeser\tstatsSize"
        + "\tCompact\tQuant\tCDF\tRank\tSer\tDeser\tstatsSize\tItems";
  }

  @Override
  public String getStats(final int streamLength, final int numTrials, final int numQueryValues) {
    return String.format("%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%d"
      + "\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%d\t%d",
      streamLength,
      numTrials,
      (double) buildTimeNs / numTrials,
      (double) updateTimeNs / numTrials / streamLength,
      (double) updateGetQuantilesTimeNs / numTrials / numQueryValues,
      (double) updateGetCdfTimeNs / numTrials / numQueryValues,
      (double) updateGetRankTimeNs / numTrials / numQueryValues,
      (double) updateSerializeTimeNs / numTrials,
      (double) updateDeserializeTimeNs / numTrials,
      updateSerializedSizeBytes / numTrials,
      (double) compactTimeNs / numTrials,
      (double) compactGetQuantilesTimeNs / numTrials / numQueryValues,
      (double) compactGetCdfTimeNs / numTrials / numQueryValues,
      (double) compactGetRankTimeNs / numTrials / numQueryValues,
      (double) compactSerializeTimeNs / numTrials,
      (double) compactDeserializeTimeNs / numTrials,
      compactSerializedSizeBytes / numTrials,
      numRetainedItems / numTrials
    );
  }

  private void resetStats() {
    buildTimeNs = 0;
    updateTimeNs = 0;
    updateGetQuantilesTimeNs = 0;
    updateGetCdfTimeNs = 0;
    updateGetRankTimeNs = 0;
    updateSerializeTimeNs = 0;
    updateDeserializeTimeNs = 0;
    updateSerializedSizeBytes = 0;
    compactTimeNs = 0;
    compactGetQuantilesTimeNs = 0;
    compactGetCdfTimeNs = 0;
    compactGetRankTimeNs = 0;
    compactSerializeTimeNs = 0;
    compactDeserializeTimeNs = 0;
    compactSerializedSizeBytes = 0;
    numRetainedItems = 0;
  }

}
