/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.characterization.tdigest;

import static org.apache.datasketches.common.Util.pwr2SeriesNext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.MonotonicPoints;
import org.apache.datasketches.Properties;

import com.tdunning.math.stats.TDigest;

public class TDigestMergeAccuracyProfile implements JobProfile {
  private Job job;
  private Properties prop;

  private Random rand;
  private double[] values;

  @Override
  public void start(final Job job) {
    this.job = job;
    prop = job.getProperties();
    final int lgMin = Integer.parseInt(prop.mustGet("LgMin"));
    final int lgMax = Integer.parseInt(prop.mustGet("LgMax"));
    final int lgDelta = Integer.parseInt(prop.mustGet("LgDelta"));
    final int ppo = Integer.parseInt(prop.mustGet("PPO"));

    final int compression = Integer.parseInt(prop.mustGet("Compression"));
    final double[] ranks = {0.01, 0.05, 0.5, 0.95, 0.99};
    final double errorPercentile = Double.parseDouble(prop.mustGet("ErrorPercentile"));

    rand = new Random();
    values = new double[1 << lgMax];
    
    final int numSteps;
    final boolean useppo;
    if (lgDelta < 1) {
      numSteps = MonotonicPoints.countPoints(lgMin, lgMax, ppo);
      useppo = true;
    } else {
      numSteps = (lgMax - lgMin) / lgDelta + 1;
      useppo = false;
    }

    int streamLength = 1 << lgMin;
    int lgCurSL = lgMin;
    for (int step = 0; step < numSteps; step++) {

      doStreamLength(streamLength, compression, ranks, errorPercentile);

      if (useppo) {
        streamLength = (int)pwr2SeriesNext(ppo, streamLength);
      } else {
        lgCurSL += lgDelta;
        streamLength = 1 << lgCurSL;
      }
    }
  }
  
  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

  void doStreamLength(final int streamLength, final int compression, final double[] ranks, final double errorPercentile) {
    final int numTrials = 1 << Integer.parseInt(prop.mustGet("LgTrials"));

    double[][] rankErrors = new double[ranks.length][];
    for (int i = 0; i < ranks.length; i++) rankErrors[i] = new double[numTrials];

    for (int t = 0; t < numTrials; t++) {
      runTrial(t, streamLength, compression, ranks, rankErrors);
    }

    job.print(streamLength);
    for (int i = 0; i < ranks.length; i++) {
      Arrays.sort(rankErrors[i]);
      final int errPctIndex = (int)(numTrials * errorPercentile / 100);
      final double rankErrorAtPct = rankErrors[i][errPctIndex];
      job.print("\t");
      job.print(rankErrorAtPct * 100);
    }
    job.print("\n");
  }

  void runTrial(final int trial, final int streamLength, final int compression, final double[] ranks, final double[][] rankErrors) {
    final int numSketches = Integer.parseInt(prop.mustGet("NumSketches"));
    final ArrayList<TDigest> tds = new ArrayList<>();
    for (int s = 0; s < numSketches; s++) tds.add(TDigest.createDigest(compression));
    int s = 0;
    for (int i = 0; i < streamLength; i++) {
      values[i] = rand.nextDouble(); 
      tds.get(s).add(values[i]);
      s++;
      if (s == numSketches) s = 0;
    }
    TDigest tdMerge = TDigest.createDigest(compression);
    tdMerge.add(tds);
    Arrays.sort(values, 0, streamLength);
    for (int i = 0; i < ranks.length; i++) {
      final double quantile = values[(int)((streamLength - 1) * ranks[i])];
      final double trueRank = computeTrueRank(values, streamLength, quantile);
      rankErrors[i][trial] = Math.abs(trueRank - tdMerge.cdf(quantile));
    }
  }

  static double computeTrueRank(final double[] values, final int streamLength, final double value) {
    final int lower = lowerBound(values, 0, streamLength, value);
    final int upper = upperBound(values, lower, streamLength, value);
    return (lower + upper) / 2.0 / streamLength;
  }

  static int lowerBound(final double[] values, int first, final int last, final double value) {
    int current;
    int step;
    int count = last - first; 
    while (count > 0) {
      current = first;
      step = count / 2;
      current += step;
      if (values[current] < value) {
        first = ++current;
        count -= step + 1;
      } else {
        count = step;
      }
    }
    return first;
  }

  static int upperBound(final double[] values, int first, final int last, final double value) {
    int current;
    int step;
    int count = last - first; 
    while (count > 0) {
      current = first; 
      step = count / 2; 
      current += step;
      if (!(value < values[current])) {
        first = ++current;
        count -= step + 1;
      } else {
        count = step;
      }
    }
    return first;
  }

}
