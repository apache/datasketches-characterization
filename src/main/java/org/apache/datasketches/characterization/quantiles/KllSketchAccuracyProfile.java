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

import static org.apache.datasketches.characterization.Shuffle.shuffle;

import org.apache.datasketches.Properties;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllFloatsSketch;

/**
 * This handles either floats or doubles.
 * @author Lee Rhodes
 *
 */
public class KllSketchAccuracyProfile extends BaseQuantilesAccuracyProfile {

  private int k;
  private float[] inputFloatValues;
  private float[] floatQueryValues;
  private double[] inputDoubleValues;
  private double[] doubleQueryValues;
  private boolean useBulk;
  private String type;
  private boolean useFloat;
  private boolean useDouble;
  //add other types

  @Override
  void configure(final Properties props) {
    useFloat = useDouble = false;
    k = Integer.parseInt(props.mustGet("K"));
    useBulk = Boolean.parseBoolean(props.mustGet("useBulk"));
    type = props.mustGet("type");
    if (type.equalsIgnoreCase("float")) { useFloat = true; }
    else if (type.equalsIgnoreCase("double")) { useDouble = true; }
    else { throw new IllegalArgumentException("Unknown primitive type: " + type); }
  }

  @Override
  void prepareTrial(final int streamLength) {
    // prepare input data that will be permuted
    if (useFloat) {
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
    else if (useDouble) {
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
    }
  }

  @Override
  double doTrial() {
    double maxRankError = 0;
    if (useFloat) {
      shuffle(inputFloatValues);
      // build sketch
      final KllFloatsSketch sketch = new KllFloatsSketch(k);
      for (int i = 0; i < inputFloatValues.length; i++) {
        sketch.update(inputFloatValues[i]);
      }

      // query sketch and gather results
      maxRankError = 0;
      if (useBulk) {
        final double[] estRanks = sketch.getCDF(floatQueryValues);
        for (int i = 0; i < inputFloatValues.length; i++) {
          final double trueRank = (double) i / inputFloatValues.length;
          maxRankError = Math.max(maxRankError, Math.abs(trueRank - estRanks[i]));
        }
      } else {
        for (int i = 0; i < inputFloatValues.length; i++) {
          final double trueRank = (double) i / inputFloatValues.length;
          final double estRank = sketch.getRank(i);
          maxRankError = Math.max(maxRankError, Math.abs(trueRank - estRank));
        }
      }
    }
    else if (useDouble) {
      shuffle(inputDoubleValues);
      // build sketch
      final KllDoublesSketch sketch = new KllDoublesSketch(k);
      for (int i = 0; i < inputDoubleValues.length; i++) {
        sketch.update(inputDoubleValues[i]);
      }

      // query sketch and gather results
      maxRankError = 0;
      if (useBulk) {
        final double[] estRanks = sketch.getCDF(doubleQueryValues);
        for (int i = 0; i < inputDoubleValues.length; i++) {
          final double trueRank = (double) i / inputDoubleValues.length;
          maxRankError = Math.max(maxRankError, Math.abs(trueRank - estRanks[i]));
        }
      } else {
        for (int i = 0; i < inputDoubleValues.length; i++) {
          final double trueRank = (double) i / inputDoubleValues.length;
          final double estRank = sketch.getRank(i);
          maxRankError = Math.max(maxRankError, Math.abs(trueRank - estRank));
        }
      }
    }
    return maxRankError;
  }

}
