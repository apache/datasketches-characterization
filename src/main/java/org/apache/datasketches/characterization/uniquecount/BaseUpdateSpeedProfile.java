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

import static org.apache.datasketches.Util.pwr2LawNext;
import static java.lang.Math.log;
import static java.lang.Math.pow;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;

/**
 * @author Lee Rhodes
 */
public abstract class BaseUpdateSpeedProfile implements JobProfile {
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
    configure();
    doTrials();
    shutdown();
    cleanup();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

  @Override
  public void println(final String s) {
    job.println(s);
  }
  //end JobProfile

  /**
   * Configure the sketch
   */
  public abstract void configure();

  /**
   * Return the average update time per update for this trial
   * @param uPerTrial the number of unique updates for this trial
   * @return the average update time per update for this trial
   */
  public abstract double doTrial(final int uPerTrial);

  /**
   * Traverses all the unique axis points and performs trials(u) at each point
   * and outputs a row per unique axis point.
   *
   * @param job the given job
   * @param profile the given profile
   */
  private void doTrials() {
    final int maxU = 1 << lgMaxU;
    final int minU = 1 << lgMinU;
    int lastU = 0;
    final StringBuilder dataStr = new StringBuilder();
    println(getHeader());
    while (lastU < maxU) { //Trials for each U point on X-axis, and one row on output
      final int nextU = (lastU == 0) ? minU : pwr2LawNext(uPPO, lastU);
      lastU = nextU;
      final int trials = getNumTrials(nextU);

      System.gc(); //much slower but cleaner plots
      double sumUpdateTimePerU_nS = 0;
      for (int t = 0; t < trials; t++) {
        sumUpdateTimePerU_nS += doTrial(nextU);
      }
      final double meanUpdateTimePerU_nS = sumUpdateTimePerU_nS / trials;
      process(meanUpdateTimePerU_nS, trials, nextU, dataStr);
      println(dataStr.toString());
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
    if ((lgMinT == lgMaxT) || (curU <= (minBpU))) {
      return maxT;
    }
    if (curU >= maxBpU) {
      return minT;
    }
    final double lgCurU = log(curU) / LN2;
    final double lgTrials = (slope * (lgCurU - lgMinBpU)) + lgMaxT;
    return (int) pow(2.0, lgTrials);
  }

  /**
   * Process the results
   *
   * @param statsArr the input Stats array
   * @param uPerTrial the number of uniques per trial for this trial set.
   * @param sb The StringBuilder object that is reused for each row of output
   */
  private static void process(final double meanUpdateTimePerU_nS, final int trials,
      final int uPerTrial, final StringBuilder sb) {
    // OUTPUT
    sb.setLength(0);
    sb.append(uPerTrial).append(TAB);
    sb.append(trials).append(TAB);
    sb.append(meanUpdateTimePerU_nS);
  }

  /**
   * Returns a column header row
   *
   * @return a column header row
   */
  private static String getHeader() {
    final StringBuilder sb = new StringBuilder();
    sb.append("InU").append(TAB);
    sb.append("Trials").append(TAB);
    sb.append("nS/u");
    return sb.toString();
  }

}
