/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

import static com.yahoo.sketches.Util.pwr2LawNext;
import static java.lang.Math.log;
import static java.lang.Math.pow;

import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.characterization.Properties;

/**
 * @author Lee Rhodes
 */
public abstract class BaseSerDeProfile implements JobProfile {
  Job job;
  Properties prop;
  long vIn = 0;
  int lgMinT;
  int lgMaxT;
  int lgMinU;
  int lgMaxU;
  int uPPO;
  int lgMinBpU;
  int lgMaxBpU;
  double slope;
  int lgK;

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
  }

  @Override
  public void println(final String s) {
    job.println(s);
  }

  abstract void configure();

  abstract void doTrial(SerDeStats stats, int uPerTrial);

  private void doTrials() {
    final int maxU = 1 << lgMaxU;
    final int minU = 1 << lgMinU;
    int lastU = 0;
    final StringBuilder dataStr = new StringBuilder();
    println(getHeader());
    while (lastU < maxU) { //for each U point on X-axis, OR one row on output
      final int nextU = (lastU == 0) ? minU : pwr2LawNext(uPPO, lastU);
      lastU = nextU;
      final int trials = getNumTrials(nextU);
      //Build stats arr
      final SerDeStats[] statsArr = new SerDeStats[trials];
      for (int t = 0; t < trials; t++) {
        final SerDeStats stats = statsArr[t];
        if (stats == null) {
          statsArr[t] = new SerDeStats();
        }
      }

      System.gc();
      for (int t = 0; t < trials; t++) {
        doTrial(statsArr[t], nextU); //at this # of uniques
      }
      process(statsArr, trials, nextU, dataStr);
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
  int getNumTrials(final int curU) {
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

  private static void process(final SerDeStats[] statsArr, final int trials, final int uPerTrial,
      final StringBuilder dataStr) {
    double sumSerTime_nS = 0;
    double sumDeserTime_nS = 0;

    for (int t = 0; t < trials; t++) {
      sumSerTime_nS += statsArr[t].serializeTime_nS;
      sumDeserTime_nS += statsArr[t].deserializeTime_nS;
    }
    final double meanSerTime_nS = sumSerTime_nS / trials;
    final double meanDeserTime_ns = sumDeserTime_nS / trials;

    //OUTPUT
    dataStr.setLength(0);
    dataStr.append(uPerTrial).append(TAB);
    dataStr.append(trials).append(TAB);
    dataStr.append(meanSerTime_nS).append(TAB);
    dataStr.append(meanDeserTime_ns);
  }

  private static String getHeader() {
    final StringBuilder sb = new StringBuilder();
    sb.append("InU").append(TAB);
    sb.append("Trials").append(TAB);
    sb.append("Ser_nS").append(TAB);
    sb.append("DeSer_nS");
    return sb.toString();
  }

}
