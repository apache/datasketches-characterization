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

import static org.apache.datasketches.characterization.Shuffle.shuffle;
import static org.apache.datasketches.kll.KllSketch.getNormalizedRankError;

import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.characterization.quantiles.BaseQuantilesAccuracyProfile;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This captures the maximum rank error over T trials.
 * The stream is shuffled prior to each trial.
 * This handles either floats or doubles.
 * The input distribution can be either uniform random or contiguous integral values.
 * The input is shuffled for each trial.
 *
 * @author Lee Rhodes
 *
 */
public class KllSketchAccuracyProfile extends BaseQuantilesAccuracyProfile {
  public static Random random = new Random();
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();
  private int k;
  private int streamLength;
  private float[] inputFloatValues;
  private float[] floatQueryValues;
  private double[] inputDoubleValues;
  private double[] doubleQueryValues;
  private int[] trueRanks;
  private boolean useBulk;
  private String dataType;
  private boolean direct = false;    //heap is default
  private boolean useDouble = false; //useFloat is default
  private String distributionType;
  private boolean uniformRandom = false;   //contiguous is default;
  private double epsilon;
  //add other types

  KllDoublesSketch dskUT = null;
  KllFloatsSketch fskUT = null;

  @Override //BaseQuantilesAccuracyProfile
  public void configure() {
    k = Integer.parseInt(props.mustGet("K"));
    epsilon = getNormalizedRankError(k, false);
    useBulk = Boolean.parseBoolean(props.mustGet("useBulk"));
    direct = Boolean.parseBoolean(props.mustGet("direct"));
    dataType = props.mustGet("dataType");
    if (dataType.equalsIgnoreCase("double")) { useDouble = true; }
    distributionType = props.mustGet("distributionType");
    if (distributionType.equalsIgnoreCase("uniformRandom")) { uniformRandom = true; }
    configureSketchUnderTest();
  }

  private void configureSketchUnderTest() {
    if (useDouble) {
      if (direct) {
        final WritableMemory dstMem = WritableMemory.allocate(10000);
        dskUT = KllDoublesSketch.newDirectInstance(k, dstMem, memReqSvr);
      } else { //heap
        dskUT = KllDoublesSketch.newHeapInstance(k);
      }
    } else { //useFloat
      if (direct) {
        final WritableMemory dstMem = WritableMemory.allocate(10000);
        fskUT = KllFloatsSketch.newDirectInstance(k, dstMem, memReqSvr);
      } else { //heap
        fskUT = KllFloatsSketch.newHeapInstance(k);
      }
    }
  }

  @Override //BaseQuantilesAccuracyProfile
  // called once per streamLength.
  //prepare input data that will be permuted and associated arrays
  public void prepareTrialSet(final int streamLength) {
    this.streamLength = streamLength;
    final int[] sortedArr;

    if (useDouble) {
      if (uniformRandom) {
        inputDoubleValues = new double[streamLength];
        sortedArr = fillUniformRandomDoubles(inputDoubleValues);
        //Compute true ranks & compress
        doubleQueryValues = new double[streamLength];
        trueRanks = getTrueRanksDoubles(sortedArr, doubleQueryValues);
        compressDoubleQueryValues();
      }
      else { //consecutive
        inputDoubleValues = new double[streamLength];
        sortedArr = fillContiguousDoubles(inputDoubleValues);
        //Compute true ranks
        doubleQueryValues = new double[streamLength];
        trueRanks = getTrueRanksDoubles(sortedArr, doubleQueryValues);
      }
    }
    else { //useFloats
      if (uniformRandom) {
        inputFloatValues = new float[streamLength];
        sortedArr = fillUniformRandomFloats(inputFloatValues);
        //Compute true ranks
        floatQueryValues = new float[streamLength];
        trueRanks = getTrueRanksFloats(sortedArr, floatQueryValues);
        compressFloatQueryValues();
      }
      else { //consecutive
        inputFloatValues = new float[streamLength];
        sortedArr = fillContiguousFloats(inputFloatValues);
        //Compute true ranks
        floatQueryValues = new float[streamLength];
        trueRanks = getTrueRanksFloats(sortedArr, floatQueryValues);
      }
    }
  }

  @Override
  public double getEpsilon() {
    return epsilon;
  }

  /**
   * For each trial:
   * <ul>
   * <li>Feed the sketch under test (skUT) a shuffled stream of length SL of "true" values.</li>
   * <li>Query the skUT for the estimated rank of each true value and capture the maximum
   * error of all the estimated ranks</li>
   * <li>Return this maximum error of this trial.</li>
   * </ul>
   */
  @Override //BaseQuantilesAccuracyProfile
  public double doTrial() {
    double worstNegRankError = 0;
    double worstPosRankError = 0;

    if (useDouble) {
      shuffle(inputDoubleValues);
      // reset then update sketch
      dskUT.reset();
      for (int i = 0; i < streamLength; i++) {
        dskUT.update(inputDoubleValues[i]);
      }

      // query sketch and gather results
      worstNegRankError = 0;
      worstPosRankError = 0;
      final int qLen = trueRanks.length;
      if (useBulk) {
        final double[] estRanks = dskUT.getCDF(doubleQueryValues);
        for (int i = 0; i < qLen; i++) {
          final double trueRank = (double) trueRanks[i] / streamLength;
          final double deltaRankErr = estRanks[i] - trueRank;
          if (deltaRankErr < 0) { worstNegRankError = Math.min(worstNegRankError, deltaRankErr); }
          else { worstPosRankError = Math.max(worstPosRankError, deltaRankErr); }
        }
      } else {
        for (int i = 0; i < qLen; i++) {
          final double trueRank = (double) trueRanks[i] / streamLength;
          final double deltaRankErr = dskUT.getRank(i) - trueRank;
          if (deltaRankErr < 0) { worstNegRankError = Math.min(worstNegRankError, deltaRankErr); }
          else { worstPosRankError = Math.max(worstPosRankError, deltaRankErr); }
        }
      }
    }

    else { //use Float
      shuffle(inputFloatValues);
      // reset then update sketch
      fskUT.reset();
      for (int i = 0; i < inputFloatValues.length; i++) {
        fskUT.update(inputFloatValues[i]);
      }

      // query sketch and gather results
      worstNegRankError = 0;
      worstPosRankError = 0;
      final int qLen = trueRanks.length;
      if (useBulk) {
        final double[] estRanks = fskUT.getCDF(floatQueryValues);
        for (int i = 0; i < qLen; i++) {
          final double trueRank = (double) trueRanks[i] / streamLength;
          final double deltaRankErr = estRanks[i] - trueRank;
          if (deltaRankErr < 0) { worstNegRankError = Math.min(worstNegRankError, deltaRankErr); }
          else { worstPosRankError = Math.max(worstPosRankError, deltaRankErr); }
        }
      } else {
        for (int i = 0; i < qLen; i++) {
          final double trueRank = (double) trueRanks[i] / streamLength;
          final double deltaRankErr = fskUT.getRank(i) - trueRank;
          if (deltaRankErr < 0) { worstNegRankError = Math.min(worstNegRankError, deltaRankErr); }
          else { worstPosRankError = Math.max(worstPosRankError, deltaRankErr); }
        }
      }
    }
    return (worstPosRankError > -worstNegRankError) ? worstPosRankError : worstNegRankError;
  }

  //************************************************

  /**
   * Create a set of <i>n</i> uniform random integral values in the range [0, n).
   * They will not be sorted. There may be duplicates and, therefore, missing values.
   * @param inputDoubleValues empty double[] of size <i>n</i>.
   * @return sorted int[] of source integers
   */
  static final int[] fillUniformRandomDoubles(final double[] inputDoubleValues) {
    final int n = inputDoubleValues.length;
    final int[] valueSourceArr = new int[n];
    for (int i = 0; i < n; i++) {
      final int v = random.nextInt(n);
      valueSourceArr[i] = v;
      inputDoubleValues[i] = v;
    }
    final int[] sortedArr = valueSourceArr.clone();
    Arrays.sort(sortedArr);
    return sortedArr;
  }

  /**
   * Create a set of <i>n</i> contiguous integral values in the range [0, n)..
   * They may be sorted, but don't need to be. There are no duplicates and no gaps.
   * @param inputDoubleValues empty double[] of size <i>n</i>.
   * @return sorted int[] of source integers
   */
  static final int[] fillContiguousDoubles(final double[] inputDoubleValues) {
    final int n = inputDoubleValues.length;
    final int[] valueSourceArr = new int[n];
    for (int i = 0; i < n; i++) {
      valueSourceArr[i] = i;
      inputDoubleValues[i] = i;
    }
    final int[] sortedArr = valueSourceArr.clone();
    return sortedArr;
  }

  /**
   * @param sortedArr of source integers of size <i>n</i>.
   * @param doubleQueryValues empty double[] of size <i>n</i>.
   * @return int[] of true ranks aligned to doubleQueryValues
   */
  static final int[] getTrueRanksDoubles(final int[] sortedArr, final double[] doubleQueryValues) {
    final int n = doubleQueryValues.length;
    final int[] trueRanks = new int[n];
    int r;
    trueRanks[0] = r = 0;
    doubleQueryValues[0] = sortedArr[0];
    for (int i = 1; i < n; i++) {
      final int v;
      doubleQueryValues[i] = v = sortedArr[i];
      trueRanks[i] = (v == sortedArr[i - 1]) ? r : (r = i);
    }
    return trueRanks;
  }

  final void compressDoubleQueryValues() {
    //find num duplicates
    final int n = trueRanks.length;
    int dups = 0;
    final int[] tmpTrueRanks;
    final double[] tmpDoubleQueryValues;
    for (int i = 1; i < n; i++) {
      if (trueRanks[i] == trueRanks[i - 1]) { dups++; }
    }
    if (dups == 0) { return; }
    final int cLen = n - dups;
    tmpTrueRanks = new int[cLen];
    tmpDoubleQueryValues = new double[cLen];
    int j = 0;
    tmpTrueRanks[0] = trueRanks[0];
    tmpDoubleQueryValues[0] = doubleQueryValues[0];
    for (int i = 1; i < n; i++) {
      if (trueRanks[i] == trueRanks[i - 1]) { continue; }
      j++;
      tmpTrueRanks[j] = trueRanks[i];
      tmpDoubleQueryValues[j] = doubleQueryValues[i];
    }
    assert j + 1 == cLen : "(j + 1): " + (j + 1) + ", clen: " + cLen;
    trueRanks = tmpTrueRanks;
    doubleQueryValues = tmpDoubleQueryValues;
  }

  //************************************************

  /**
   * Create a set of <i>n</i> uniform random integral values in the range [0, n).
   * They will not be sorted. There may be duplicates and, therefore, missing values.
   * @param inputFloatValues empty float[] of size <i>n</i>.
   * @return sorted int[] of source integers
   */
  static final int[] fillUniformRandomFloats(final float[] inputFloatValues) {
    final int n = inputFloatValues.length;
    final int[] valueSourceArr = new int[n];
    for (int i = 0; i < n; i++) {
      final int v = random.nextInt(n);
      valueSourceArr[i] = v;
      inputFloatValues[i] = v;
    }
    final int[] sortedArr = valueSourceArr.clone();
    Arrays.sort(sortedArr);
    return sortedArr;
  }

  /**
   * Create a set of <i>n</i> contiguous integral values in the range [0, n).
   * They may be sorted, but don't need to be. There are no duplicates and no gaps.
   * @param inputFloatValues empty float[] of size <i>n</i>.
   * @return sorted int[] of source integers
   */
  static final int[] fillContiguousFloats(final float[] inputFloatValues) {
    final int n = inputFloatValues.length;
    final int[] valueSourceArr = new int[n];
    for (int i = 0; i < n; i++) {
      valueSourceArr[i] = i;
      inputFloatValues[i] = i;
    }
    final int[] sortedArr = valueSourceArr.clone();
    return sortedArr;
  }

  /**
   * @param sortedArr int[] of source integers of size <i>n</i>.
   * @param floatQueryValues empty float[] of size <i>n</i>.
   * @return int[] of true ranks aligned to floatQueryValues
   */
  static final int[] getTrueRanksFloats(final int[] sortedArr, final float[] floatQueryValues) {
    final int n = floatQueryValues.length;
    final int[] trueRanks = new int[n];
    int r;
    trueRanks[0] = r = 0;
    floatQueryValues[0] = sortedArr[0];
    for (int i = 1; i < n; i++) {
      final int v;
      floatQueryValues[i] = v = sortedArr[i];
      trueRanks[i] = (v == sortedArr[i - 1]) ? r : (r = i);
    }
    return trueRanks;
  }

  final void compressFloatQueryValues() {
    //find num duplicates
    final int n = trueRanks.length;
    int dups = 0;
    final int[] tmpTrueRanks;
    final float[] tmpFloatQueryValues;
    for (int i = 1; i < n; i++) {
      if (trueRanks[i] == trueRanks[i - 1]) { dups++; }
    }
    if (dups == 0) { return; }
    final int cLen = n - dups;
    tmpTrueRanks = new int[cLen];
    tmpFloatQueryValues = new float[cLen];
    int j = 0;
    tmpTrueRanks[0] = trueRanks[0];
    tmpFloatQueryValues[0] = floatQueryValues[0];
    for (int i = 1; i < n; i++) {
      if (trueRanks[i] == trueRanks[i - 1]) { continue; }
      j++;
      tmpTrueRanks[j] = trueRanks[i];
      tmpFloatQueryValues[j] = floatQueryValues[i];
    }
    assert j + 1 == cLen : "(j + 1): " + (j + 1) + ", clen: " + cLen;
    trueRanks = tmpTrueRanks;
    floatQueryValues = tmpFloatQueryValues;
  }

}
