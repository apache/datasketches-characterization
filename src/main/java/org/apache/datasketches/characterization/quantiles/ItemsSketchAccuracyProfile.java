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

import java.util.Comparator;
import java.util.Random;

import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.ItemsSketch;

public class ItemsSketchAccuracyProfile extends BaseQuantilesAccuracyProfile {

  private int lgK;
  private int k;
  private int[] inputValues;
  private Integer[] queryValues;
  private boolean useBulk;

  @Override
  public void configure() {
    lgK = Integer.parseInt(props.mustGet("lgK"));
    k = 1 << lgK;
  }

  @Override
  public double getEpsilon() {
    return DoublesSketch.getNormalizedRankError(k, false);
  }

  @Override
  public void prepareTrialSet(final int streamLength) {
    // prepare input data that will be permuted
    inputValues = new int[streamLength];
    for (int i = 0; i < streamLength; i++) {
      inputValues[i] = i;
    }
    if (useBulk) {
      // prepare query data that must remain ordered
      queryValues = new Integer[streamLength];
      for (int i = 0; i < streamLength; i++) {
        queryValues[i] = i;
      }
    }
  }

  @Override
  public double doTrial() {
    shuffle(inputValues);

    // build sketch
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(1 << lgK, Comparator.naturalOrder());
    for (int i = 0; i < inputValues.length; i++) {
      sketch.update(inputValues[i]);
    }

    // query sketch and gather results
    double maxRankError = 0;
    if (useBulk) {
      final double[] estRanks = sketch.getCDF(queryValues);
      for (int i = 0; i < inputValues.length; i++) {
        final double trueRank = (double) i / inputValues.length;
        maxRankError = Math.max(maxRankError, Math.abs(trueRank - estRanks[i]));
      }
    } else {
      for (int i = 0; i < inputValues.length; i++) {
        final double trueRank = (double) i / inputValues.length;
        //final double estRank = sketch.getRank(i); // this was not released yet
        final double estRank = sketch.getCDF(new Integer[] {i})[0];
        maxRankError = Math.max(maxRankError, Math.abs(trueRank - estRank));
      }
    }
    return maxRankError;
  }

  static final Random rnd = new Random();

  private static void shuffle(final int[] array) {
    for (int i = 0; i < array.length; i++) {
      final int r = rnd.nextInt(i + 1);
      swap(array, i, r);
    }
  }

  private static void swap(final int[] array, final int i1, final int i2) {
    final int value = array[i1];
    array[i1] = array[i2];
    array[i2] = value;
  }

}
