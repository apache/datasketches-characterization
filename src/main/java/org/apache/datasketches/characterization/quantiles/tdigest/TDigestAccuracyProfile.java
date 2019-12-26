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

package org.apache.datasketches.characterization.quantiles.tdigest;

import java.util.Arrays;

import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.quantiles.tdigest.DataGenerator.Mode;

import com.tdunning.math.stats.TDigest;

public class TDigestAccuracyProfile extends QuantilesAccuracyProfile {

  private int compression;
  private double[] inputValues;
  private DataGenerator gen;
  private DoubleRankCalculator.Mode rankMode;

  @Override
  void configure(final Properties props) {
    compression = Integer.parseInt(props.mustGet("compression"));
    gen = new DataGenerator(Mode.valueOf(props.mustGet("data")));
    rankMode = DoubleRankCalculator.Mode.valueOf(props.mustGet("rank"));
  }

  @Override
  void prepareTrial(final int streamLength) {
    inputValues = new double[streamLength];
  }

  @Override
  double doTrial() {
    gen.fillArray(inputValues);

    // build sketch
    final TDigest sketch = TDigest.createDigest(compression);
    for (int i = 0; i < inputValues.length; i++) {
      sketch.add(inputValues[i]);
    }

    Arrays.sort(inputValues);

    // query sketch and gather results
    double maxRankError = 0;
    final DoubleRankCalculator rank = new DoubleRankCalculator(inputValues, rankMode);
    for (int i = 0; i < inputValues.length; i++) {
      final double trueRank = rank.getRank(inputValues[i]);
      final double estRank = sketch.cdf(inputValues[i]);
      maxRankError = Math.max(maxRankError, Math.abs(trueRank - estRank));
    }
    return maxRankError;
  }

  @Override
  public void shutdown() {
    // TODO Auto-generated method stub

  }

  @Override
  public void cleanup() {
    // TODO Auto-generated method stub

  }

}
