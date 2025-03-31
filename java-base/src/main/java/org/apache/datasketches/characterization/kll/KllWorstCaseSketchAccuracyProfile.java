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

import static org.apache.datasketches.Constants.EIGHT_PEBIBYTE;
import static org.apache.datasketches.Constants.SIXTEEN_MEBIBYTE;
import static org.apache.datasketches.characterization.Shuffle.shuffle;
import static org.apache.datasketches.kll.KllSketch.getNormalizedRankError;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

import org.apache.datasketches.DoubleIntIntTriple;
import org.apache.datasketches.FloatIntIntTriple;
import org.apache.datasketches.SpecialRandom;
import org.apache.datasketches.characterization.quantiles.BaseQuantilesAccuracyProfile;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.kll.KllSketch;
import org.apache.datasketches.kll.KllSketch.SketchType;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

/**
 * This captures the maximum rank error over T trials.
 * This handles either floats or doubles.
 * The input distribution is integral values [1..n] (as either floats or doubles) that are shuffled for each trial.
 * The input is shuffled for each trial.
 *
 * @author Lee Rhodes
 *
 */
public class KllWorstCaseSketchAccuracyProfile extends BaseQuantilesAccuracyProfile {
  private static SpecialRandom random = new SpecialRandom();
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();
  private int k;
  private int streamLength;
  private FloatIntIntTriple[] inputFloatValues;
  private float[] floatQueryValues;
  private int[] floatRankValues;
  private DoubleIntIntTriple[] inputDoubleValues;
  private double[] doubleQueryValues;
  private int[] doubleRankValues;
  private boolean useBulk;

  private boolean direct = false;    //heap is default

  private String distributionType;
  private boolean contiguous = true;   //contiguous is default;
  private double epsilon;
  private QuantileSearchCriteria criteria;
  private SketchType sketchType;
  //add other types

  KllDoublesSketch dskUT = null;
  KllFloatsSketch fskUT = null;

  @Override //BaseQuantilesAccuracyProfile
  public void configure() {
    k = Integer.parseInt(props.mustGet("K"));
    epsilon = getNormalizedRankError(k, false);
    useBulk = Boolean.parseBoolean(props.mustGet("useBulk"));
    direct = Boolean.parseBoolean(props.mustGet("direct"));
    final String dataType = props.mustGet("dataType");
    if ( dataType.equalsIgnoreCase("double")) { sketchType = SketchType.DOUBLES_SKETCH; }
    else if (dataType.equalsIgnoreCase("float")) { sketchType = SketchType.FLOATS_SKETCH; }
    else { sketchType = SketchType.ITEMS_SKETCH; }
    distributionType = props.mustGet("distributionType");
    contiguous = (distributionType.equalsIgnoreCase("contiguous")) ? true : false;
    criteria = props.mustGet("criteria").equalsIgnoreCase("INCLUSIVE") ? INCLUSIVE : EXCLUSIVE;
    configureSketchUnderTest();
  }

  private void configureSketchUnderTest() {
    final int memBytes = KllSketch.getMaxSerializedSizeBytes(k, streamLength, sketchType, true);
    if (sketchType == SketchType.DOUBLES_SKETCH) {
      if (direct) {
        final WritableMemory dstMem = WritableMemory.allocate(memBytes);
        dskUT = KllDoublesSketch.newDirectInstance(k, dstMem, memReqSvr);
      } else { //heap
        dskUT = KllDoublesSketch.newHeapInstance(k);
      }
    } else { //useFloat
      if (direct) {
        final WritableMemory dstMem = WritableMemory.allocate(memBytes);
        fskUT = KllFloatsSketch.newDirectInstance(k, dstMem, memReqSvr);
      } else { //heap
        fskUT = KllFloatsSketch.newHeapInstance(k);
      }
    } // else item
  }

  @Override //BaseQuantilesAccuracyProfile
  // called once per streamLength.
  //prepare input data that will be permuted and associated arrays
  public void prepareTrialSet(final int streamLength) {
    this.streamLength = streamLength;

    if (sketchType == SketchType.DOUBLES_SKETCH) {
      inputDoubleValues = fillDoubleArrays(streamLength, contiguous);
    }
    else { //useFloats
      inputFloatValues = fillFloatArrays(streamLength, contiguous);
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

    //DOUBLES SKETCH
    if (sketchType == SketchType.DOUBLES_SKETCH) {
      shuffle(inputDoubleValues);
      doubleQueryValues = DoubleIntIntTriple.getValueArray(inputDoubleValues);
      doubleRankValues = DoubleIntIntTriple.getRankArray(inputDoubleValues);
      // reset then update sketch
      dskUT.reset();
      for (int i = 0; i < streamLength; i++) {
        dskUT.update(inputDoubleValues[i].value);
      }

      // query sketch and gather results
      worstNegRankError = 0;
      worstPosRankError = 0;
      final int queryLen = inputDoubleValues.length;
      final int one = (criteria == EXCLUSIVE) ? 1 : 0;
      if (useBulk) {
        final double[] estRanks = dskUT.getRanks(doubleQueryValues);
        for (int i = 0; i < queryLen; i++) {
          final double trueNormRank = (double) (doubleRankValues[i] - one) / streamLength;
          final double deltaRankErr = estRanks[i] - trueNormRank;
          if (deltaRankErr < 0) { worstNegRankError = Math.min(worstNegRankError, deltaRankErr); }
          else { worstPosRankError = Math.max(worstPosRankError, deltaRankErr); }
        }
      } else {
        for (int i = 0; i < queryLen; i++) {
          final double estRank = dskUT.getRank(doubleRankValues[i]);
          final double trueNormRank = (double) (doubleRankValues[i] - one) / streamLength;
          final double deltaRankErr = estRank - trueNormRank;
          if (deltaRankErr < 0) { worstNegRankError = Math.min(worstNegRankError, deltaRankErr); }
          else { worstPosRankError = Math.max(worstPosRankError, deltaRankErr); }
        }
      }
    }
    //FLOATS SKETCH
    else {
      shuffle(inputFloatValues);
      floatQueryValues = FloatIntIntTriple.getValueArray(inputFloatValues);
      floatRankValues = FloatIntIntTriple.getRankArray(inputFloatValues);
      // reset then update sketch
      fskUT.reset();
      for (int i = 0; i < inputFloatValues.length; i++) {
        fskUT.update(inputFloatValues[i].value);
      }

      // query sketch and gather results
      worstNegRankError = 0;
      worstPosRankError = 0;
      final int queryLen = inputFloatValues.length;
      final int one = (criteria == EXCLUSIVE) ? 1 : 0;
      if (useBulk) {
        final double[] estRanks = fskUT.getRanks(floatQueryValues);
        for (int i = 0; i < queryLen; i++) {
          final double normRank = (double) (floatRankValues[i] - one) / streamLength;
          final double deltaRankErr = estRanks[i] - normRank;
          if (deltaRankErr < 0) { worstNegRankError = Math.min(worstNegRankError, deltaRankErr); }
          else { worstPosRankError = Math.max(worstPosRankError, deltaRankErr); }
        }
      } else {
        for (int i = 0; i < queryLen; i++) {
          final double estRank = fskUT.getRank(floatRankValues[i]);
          final double normRank = (double) (floatRankValues[i] - one) / streamLength;
          final double deltaRankErr = estRank - normRank;
          if (deltaRankErr < 0) { worstNegRankError = Math.min(worstNegRankError, deltaRankErr); }
          else { worstPosRankError = Math.max(worstPosRankError, deltaRankErr); }
        }
      }
    }
    return (worstPosRankError > -worstNegRankError) ? worstPosRankError : worstNegRankError;
  }

  //************************************************

  /**
   * Create a DoubleIntIntTriple of <i>n</i> integral double values with associated index and rank
   * @param n the given size of the returned filled array.
   * @param contiguous if true, the values will be from the range [1, n].
   * Otherwise, the values will be uniform random values from the range [1, 2^53] when sorted by index.
   * @return DoubleIntIntTriple array.
   */
  static final DoubleIntIntTriple[] fillDoubleArrays(final int n, final boolean contiguous) {
    final DoubleIntIntTriple[] array = new DoubleIntIntTriple[n];

    if (contiguous) {
      for (int i = 0, r = 1; i < n; i++, r++) {
        final DoubleIntIntTriple item  = array[i] = new DoubleIntIntTriple();
        item.index = i;
        item.value = r;
        item.rank = r;
      }
    } else { //random
      for (int i = 0; i < n; i++) {
        final DoubleIntIntTriple item  = array[i] = new DoubleIntIntTriple();
        item.index = i;
        item.value = random.nextLong(EIGHT_PEBIBYTE) + 1L;
      }
      DoubleIntIntTriple.sortByValue(array);
      for (int i = 0, r = 1; i < n; i++, r++) {
        array[i].rank = r;
      }
      DoubleIntIntTriple.sortByIndex(array);
    }
    return array;
  }

  /**
   * Create a FloatIntIntTriple of <i>n</i> integral float values with associated index and rank
   * @param n the given size of the returned filled array.
   * @param contiguous if true, the values will be from the range [1, n].
   * Otherwise, the values will be uniform random values from the range [1, 2^24] when the array is sorted by index.
   * @return FloatIntIntTriple array sorted by index.
   */
  static final FloatIntIntTriple[] fillFloatArrays(final int n, final boolean contiguous) {
    final FloatIntIntTriple[] array = new FloatIntIntTriple[n];

    if (contiguous) {
      for (int i = 0, r = 1; i < n; i++, r++) {
        final FloatIntIntTriple item  = array[i] = new FloatIntIntTriple();
        item.index = i;
        item.value = r;
        item.rank = r;
      }
    } else { //random
      for (int i = 0; i < n; i++) {
        final FloatIntIntTriple item  = array[i] = new FloatIntIntTriple();
        item.index = i;
        item.value = random.nextInt(SIXTEEN_MEBIBYTE) + 1;
      }
      FloatIntIntTriple.sortByValue(array);
      for (int i = 0, r = 1; i < n; i++, r++) {
        array[i].rank = r;
      }
      FloatIntIntTriple.sortByIndex(array);
    }
    return array;
  }

}
