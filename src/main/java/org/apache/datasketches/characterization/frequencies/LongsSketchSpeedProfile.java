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

package org.apache.datasketches.characterization.frequencies;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.ZipfDistribution;
import org.apache.datasketches.frequencies.LongsSketch;

public class LongsSketchSpeedProfile extends BaseFrequenciesSpeedProfile {

  private int k;
  private ZipfDistribution zipf;
  private long[] inputValues;

  long buildTimeNs;
  long updateTimeNs;
  long serializeTimeNs;
  long deserializeTimeNs;
  long numRetainedItems;
  long serializedSizeBytes;

  @Override
  void configure(final Properties properties) {
    k = Integer.parseInt(properties.mustGet("k"));
    final int range = Integer.parseInt(properties.mustGet("zipfRange"));
    final double exponent = Double.parseDouble(properties.mustGet("zipfExponent"));
    zipf = new ZipfDistribution(range, exponent);
  }

  @Override
  void prepareTrial(final int streamLength) {
    // prepare input data
    inputValues = new long[streamLength];
    for (int i = 0; i < streamLength; i++) {
      inputValues[i] = zipf.sample();
    }
  }

  @Override
  void doTrial() {
    final long startBuild = System.nanoTime();
    final LongsSketch sketch = new LongsSketch(k);
    final long stopBuild = System.nanoTime();
    buildTimeNs += stopBuild - startBuild;

    final long startUpdate = System.nanoTime();
    for (int i = 0; i < inputValues.length; i++) {
      sketch.update(inputValues[i]);
    }
    final long stopUpdate = System.nanoTime();
    updateTimeNs += stopUpdate - startUpdate;

    final long startSerialize = System.nanoTime();
    final byte[] bytes = sketch.toByteArray();
    final long stopSerialize = System.nanoTime();
    serializeTimeNs += stopSerialize - startSerialize;

    final Memory mem = Memory.wrap(bytes);
    final long startDeserialize = System.nanoTime();
    LongsSketch.getInstance(mem);
    final long stopDeserialize = System.nanoTime();
    deserializeTimeNs += stopDeserialize - startDeserialize;

    numRetainedItems += sketch.getNumActiveItems();
    serializedSizeBytes += bytes.length;
  }

  @Override
  String getHeader() {
    return "Stream\tTrials\tBuild\tUpdate\tSer\tDeser\tItems\tSize";
  }

  @Override
  String getStats(final int streamLength, final int numTrials) {
    return String.format("%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f",
      streamLength,
      numTrials,
      (double) buildTimeNs / numTrials,
      (double) updateTimeNs / numTrials / streamLength,
      (double) serializeTimeNs / numTrials,
      (double) deserializeTimeNs / numTrials,
      (double) numRetainedItems / numTrials,
      (double) serializedSizeBytes / numTrials
    );
  }

  @Override
  void resetStats() {
    buildTimeNs = 0;
    updateTimeNs = 0;
    serializeTimeNs = 0;
    deserializeTimeNs = 0;
    numRetainedItems = 0;
    serializedSizeBytes = 0;
  }

}
