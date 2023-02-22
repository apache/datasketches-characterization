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

package org.apache.datasketches.characterization.req;

import java.lang.reflect.Array;
import java.util.Random;

import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.quantiles.BaseQuantilesSpeedProfile;
import org.apache.datasketches.req.ReqSketch;
import org.apache.datasketches.req.ReqSketchBuilder;

public class ReqMergeSpeedProfile extends BaseQuantilesSpeedProfile {

  private static final Random rnd = new Random();
  private int numSketches;
  private float[] inputValues;
  private ReqSketch[] sketches;
  private ReqSketchBuilder sketchBuilder;

  long buildTimeNs;
  long updateTimeNs;
  long mergeTimeNs;
  long numRetainedItems;

  @Override
  public void configure(final int k, final int numQueryValues, final Properties properties) {
    this.numSketches = Integer.parseInt(properties.mustGet("numSketches"));
    final boolean HRA = Boolean.parseBoolean(properties.mustGet("HRA"));
    sketches = (ReqSketch[]) Array.newInstance(ReqSketch.class, numSketches);
    sketchBuilder = ReqSketch.builder().setK(k).setHighRankAccuracy(HRA);
  }

  @Override
  public void prepareTrial(final int streamLength) {
    // prepare input data
    inputValues = new float[streamLength];
    for (int i = 0; i < streamLength; i++) {
      inputValues[i] = rnd.nextFloat();
    }
    resetStats();
  }

  @Override
  public void doTrial() {
    final long startBuild = System.nanoTime();
    for (int i = 0; i < numSketches; i++) {
      sketches[i] = sketchBuilder.build();
    }
    final long stopBuild = System.nanoTime();
    buildTimeNs += stopBuild - startBuild;

    final long startUpdate = System.nanoTime();
    { // spray values across all sketches
      int i = 0;
      for (int j = 0; j < inputValues.length; j++) {
        sketches[i++].update(inputValues[j]);
        if (i == numSketches) { i = 0; }
      }
    }
    final long stopUpdate = System.nanoTime();
    updateTimeNs += stopUpdate - startUpdate;

    final ReqSketch mergedSketch = sketchBuilder.build();
    final long startMerge = System.nanoTime();
    for (int i = 0; i < numSketches; i++) {
      mergedSketch.merge(sketches[i]);
    }
    final long stopMerge = System.nanoTime();
    mergeTimeNs += stopMerge - startMerge;

    // record the last one since they must be the same
    // but let's average across all trials to see if there is an anomaly
    numRetainedItems += mergedSketch.getNumRetained();
  }

  @Override
  public String getHeader() {
    return "Stream\tTrials\tBuild\tUpdate\tMerge\tItems";
  }

  @Override
  public String getStats(final int streamLength, final int numTrials, final int numQueryValues) {
    return String.format("%d\t%d\t%.1f\t%.1f\t%.1f\t%d",
      streamLength,
      numTrials,
      (double) buildTimeNs / numTrials,
      (double) updateTimeNs / numTrials / streamLength,
      (double) mergeTimeNs / numTrials,
      numRetainedItems / numTrials
    );
  }

  private void resetStats() {
    buildTimeNs = 0;
    updateTimeNs = 0;
    mergeTimeNs = 0;
    numRetainedItems = 0;
  }

}
