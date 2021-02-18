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

package org.apache.datasketches.characterization.uniquecount;

import static java.lang.Math.log;
import static java.lang.Math.pow;
import static org.apache.datasketches.Util.pwr2LawNext;

import java.util.Arrays;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;

/**
 * @author Lee Rhodes
 */
public abstract class BaseSerDeProfile implements JobProfile {
  Job job;
  public Properties prop;
  public long vIn = 0;
  int lgMinT;
  int lgMaxT;
  int lgMinU;
  int lgMaxU;
  int uPPO;
  int lgMinBpU;
  int lgMaxBpU;
  double slope;
  public int lgK;

  //stat measurements
  public static final int ser_ns = 0;
  public static final int deser_ns = 1;
  public static final int est_ns = 2;
  public static final int size_bytes = 3;
  public static final int numStats = 4;

  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
    prop = job.getProperties();
    lgMinT = Integer.parseInt(prop.mustGet("Trials_lgMinT"));
    lgMaxT = Integer.parseInt(prop.mustGet("Trials_lgMaxT"));
    lgMinU = Integer.parseInt(prop.mustGet("Trials_lgMinU"));
    lgMaxU = Integer.parseInt(prop.mustGet("Trials_lgMaxU"));
    uPPO = Integer.parseInt(prop.mustGet("Trials_UPPO"));
    lgMinBpU = Integer.parseInt(prop.mustGet("Trials_lgMinBpU"));
    lgMaxBpU = Integer.parseInt(prop.mustGet("Trials_lgMaxBpU"));
    slope = (double) (lgMaxT - lgMinT) / (lgMinBpU - lgMaxBpU);
    lgK = Integer.parseInt(prop.mustGet("LgK"));
    configure();
    doTrials();
    shutdown();
    cleanup();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}
  //end JobProfile

  /**
   * Configure the sketch
   */
  public abstract void configure();

  /**
   * Populates the sketch with the given uPerTrial and loads the stats.
   * @param stats the given stats array
   * @param uPerTrial the given uniques per trial to be offered
   */
  public abstract void doTrial(long[] stats, int uPerTrial);

  private void doTrials() {
    final int maxU = 1 << lgMaxU;
    final int minU = 1 << lgMinU;
    int lastU = 0;
    final StringBuilder dataStr = new StringBuilder();
    final long[] rawStats = new long[numStats];
    final long[] sumStats = new long[numStats];
    final double[] meanStats = new double[numStats];
    job.println(getHeader());

    while (lastU < maxU) { //for each U point on X-axis, OR one row on output
      final int nextU = lastU == 0 ? minU : pwr2LawNext(uPPO, lastU);
      lastU = nextU;
      final int trials = getNumTrials(nextU);

      Arrays.fill(sumStats, 0);

      System.gc(); //much slower but cleaner plots
      for (int t = 0; t < trials; t++) {
        doTrial(rawStats, nextU); //at this # of uniques
        for (int i = 0; i < numStats; i++) {
          sumStats[i] += rawStats[i];
        }
      }
      for (int i = 0; i < numStats; i++) {
        meanStats[i] = (double)sumStats[i] / trials;
      }
      process(meanStats, trials, nextU, dataStr);
      job.println(dataStr.toString());
    }
  }

  /**
   * Computes the number of trials for a given current number of uniques for a
   * trial set. This is used in speed trials and decreases the number of trials
   * as the number of uniques increase.
   *
   * @param curU the given current number of uniques for a trial set.
   * @return the number of trials for a given current number of uniques for a
   * trial set.
   */
  private int getNumTrials(final int curU) {
    final int minBpU = 1 << lgMinBpU;
    final int maxBpU = 1 << lgMaxBpU;
    final int maxT = 1 << lgMaxT;
    final int minT = 1 << lgMinT;
    if (lgMinT == lgMaxT || curU <= minBpU) {
      return maxT;
    }
    if (curU >= maxBpU) {
      return minT;
    }
    final double lgCurU = log(curU) / LN2;
    final double lgTrials = slope * (lgCurU - lgMinBpU) + lgMaxT;
    return (int) pow(2.0, lgTrials);
  }

  private static void process(final double[] meanStats, final int trials, final int uPerTrial,
      final StringBuilder dataStr) {
    //OUTPUT
    dataStr.setLength(0); //reset
    dataStr.append(uPerTrial).append(TAB);
    dataStr.append(trials).append(TAB);
    dataStr.append(String.format("%12.1f", meanStats[ser_ns])).append(TAB);
    dataStr.append(String.format("%12.1f", meanStats[deser_ns])).append(TAB);
    dataStr.append(String.format("%12.1f", meanStats[est_ns])).append(TAB);
    dataStr.append(String.format("%12.1f", meanStats[size_bytes]));
  }

  private static String getHeader() {
    final StringBuilder sb = new StringBuilder();
    sb.append("InU").append(TAB);
    sb.append("Trials").append(TAB);
    sb.append("Ser_nS").append(TAB);
    sb.append("DeSer_nS").append(TAB);
    sb.append("Est_nS").append(TAB);
    sb.append("Size_B");
    return sb.toString();
  }

}
