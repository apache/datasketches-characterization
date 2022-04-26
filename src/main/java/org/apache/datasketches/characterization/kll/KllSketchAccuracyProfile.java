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

import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.quantiles.BaseQuantilesAccuracyProfile;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * This captures the maximum rank error over T trials.
 * The stream is shuffled prior to each trial.
 * This handles either floats or doubles.
 * @author Lee Rhodes
 *
 */
public class KllSketchAccuracyProfile extends BaseQuantilesAccuracyProfile {
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();
  private int k;
  private float[] inputFloatValues;
  private float[] floatQueryValues;
  private double[] inputDoubleValues;
  private double[] doubleQueryValues;
  private boolean useBulk;
  private String type;
  private boolean direct = false;    //heap is default
  private boolean useDouble = false; //useFloat is default
  //add other types

  KllDoublesSketch dskUT = null;
  KllFloatsSketch fskUT = null;

  @Override //BaseQuantilesAccuracyProfile
  public void configure(final Properties props) {
    k = Integer.parseInt(props.mustGet("K"));
    useBulk = Boolean.parseBoolean(props.mustGet("useBulk"));
    direct = Boolean.parseBoolean(props.mustGet("direct"));
    type = props.mustGet("type");
    if (type.equalsIgnoreCase("double")) { useDouble = true; }

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
  public void prepareTrial(final int streamLength) {
    // prepare input data that will be permuted
    if (useDouble) {
      inputDoubleValues = new double[streamLength];
      for (int i = 0; i < streamLength; i++) {
        inputDoubleValues[i] = i;
      }
      if (useBulk) {
        // prepare query data that must remain ordered
        doubleQueryValues = new double[streamLength];
        for (int i = 0; i < streamLength; i++) {
          doubleQueryValues[i] = i;
        }
      }
    } else { //use Float
      inputFloatValues = new float[streamLength];
      for (int i = 0; i < streamLength; i++) {
        inputFloatValues[i] = i;
      }
      if (useBulk) {
        // prepare query data that must remain ordered
        floatQueryValues = new float[streamLength];
        for (int i = 0; i < streamLength; i++) {
          floatQueryValues[i] = i;
        }
      }
    }
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
      for (int i = 0; i < inputDoubleValues.length; i++) {
        dskUT.update(inputDoubleValues[i]);
      }

      // query sketch and gather results
      worstNegRankError = 0;
      worstPosRankError = 0;
      if (useBulk) {
        final double[] estRanks = dskUT.getCDF(doubleQueryValues);
        for (int i = 0; i < inputDoubleValues.length; i++) {
          final double trueRank = (double) i / inputDoubleValues.length;
          final double deltaRankErr = estRanks[i] - trueRank;
          if (deltaRankErr < 0) { worstNegRankError = Math.min(worstNegRankError, deltaRankErr); }
          else { worstPosRankError = Math.max(worstPosRankError, deltaRankErr); }
        }
      } else {
        for (int i = 0; i < inputDoubleValues.length; i++) {
          final double trueRank = (double) i / inputDoubleValues.length;
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
      if (useBulk) {
        final double[] estRanks = fskUT.getCDF(floatQueryValues);
        for (int i = 0; i < inputFloatValues.length; i++) {
          final double trueRank = (double) i / inputFloatValues.length;
          final double deltaRankErr = estRanks[i] - trueRank;
          if (deltaRankErr < 0) { worstNegRankError = Math.min(worstNegRankError, deltaRankErr); }
          else { worstPosRankError = Math.max(worstPosRankError, deltaRankErr); }
        }
      } else {
        for (int i = 0; i < inputFloatValues.length; i++) {
          final double trueRank = (double) i / inputFloatValues.length;
          final double deltaRankErr = fskUT.getRank(i) - trueRank;
          if (deltaRankErr < 0) { worstNegRankError = Math.min(worstNegRankError, deltaRankErr); }
          else { worstPosRankError = Math.max(worstPosRankError, deltaRankErr); }
        }
      }
    }
    return (worstPosRankError > -worstNegRankError) ? worstPosRankError : worstNegRankError;
  }

}
