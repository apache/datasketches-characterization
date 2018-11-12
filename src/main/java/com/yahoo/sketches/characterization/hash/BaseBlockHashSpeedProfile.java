/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.hash;

import static com.yahoo.sketches.Util.pwr2LawNext;
import static java.lang.Math.log;
import static java.lang.Math.pow;

import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.characterization.Properties;

/**
 * @author Lee Rhodes
 */
public abstract class BaseBlockHashSpeedProfile implements JobProfile {
  Job job;
  Properties prop;
  long vIn = 0;
  int lgMinT;
  int lgMaxT;
  int lgMinX; //longs
  int lgMaxX; //longs
  int xPPO;
  int lgMinBpX; //longs for reducing T
  int lgMaxBpX; //longs for reducing T
  double slope;
  Point p;

  static class Point {
    int longsX  ;
    int trials;
    long sumTrials_nS = 0;
    long sumTrialsFill_nS = 0;
    long sumTrialsMemHash_nS = 0;
    long sumTrialsArrHash_nS = 0;

    Point(final int longsX, final int trials) {
      this.longsX = longsX;
      this.trials = trials;
    }

    public static String getHeader() {
      final String s =
            "LgBytes" + TAB
          + "Trials" + TAB
          + "Trial_nS" + TAB
          + "Fill_nS" + TAB
          + "Mem_nS" + TAB
          + "Arr_nS" + TAB
          + "Fill_B/S" + TAB
          + "Mem_B/S" + TAB
          + "Arr_B/S";
      return s;
    }

    public String getRow() {
      final long bytesX = longsX << 3;
      final double lgBytesX = Math.log(bytesX) / LN2;
      final double avgTrial_nS = (double)sumTrials_nS / trials;
      final double avgTrialFill_nS = (double)sumTrialsFill_nS / trials;
      final double avgTrialMemHash_nS = (double)sumTrialsMemHash_nS / trials;
      final double avgTrialArrHash_nS = (double)sumTrialsArrHash_nS / trials;
      final double fillBytesPerSec = bytesX / (avgTrialFill_nS / 1e9);
      final double memHashBytesPerSec = bytesX / (avgTrialMemHash_nS / 1e9);
      final double arrHashBytesPerSec = bytesX / (avgTrialArrHash_nS / 1e9);
      final String out = String.format(
          "%9.2f\t"  //LgBytes
        + "%d\t"     //Trials
        + "%d\t"     //Trail ns
        + "%d\t"     //Fill ns
        + "%d\t"     //Mem ns
        + "%d\t"     //Arr ns
        + "%9.0f\t"  //Fill rate
        + "%9.0f\t"  //Mem rate
        + "%9.0f\t", //Arr rate
          lgBytesX,
          trials,
          avgTrial_nS,
          avgTrialFill_nS,
          avgTrialMemHash_nS,
          avgTrialArrHash_nS,
          fillBytesPerSec,
          memHashBytesPerSec,
          arrHashBytesPerSec);
      return out;
    }
  }

  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
    prop = job.getProperties();
    lgMinT = Integer.parseInt(prop.mustGet("Trials_lgMinT"));
    lgMaxT = Integer.parseInt(prop.mustGet("Trials_lgMaxT"));
    lgMinX = Integer.parseInt(prop.mustGet("Trials_lgMinX"));
    lgMaxX = Integer.parseInt(prop.mustGet("Trials_lgMaxX"));
    xPPO = Integer.parseInt(prop.mustGet("Trials_XPPO"));
    lgMinBpX = Integer.parseInt(prop.mustGet("Trials_lgMinBpX"));
    lgMaxBpX = Integer.parseInt(prop.mustGet("Trials_lgMaxBpX"));
    slope = (double) (lgMaxT - lgMinT) / (lgMinBpX - lgMaxBpX);
    doTrials();
    close();
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

  abstract void configure();

  abstract void doTrial();

  abstract void close();

  private void doTrials() { //per row
    println(Point.getHeader());
    final int maxLongsX = 1 << lgMaxX;
    final int minLongsX = 1 << lgMinX;
    int lastLongsX = 0;
    while (lastLongsX < maxLongsX) {
      final int nextLongsX = (lastLongsX == 0) ? minLongsX : pwr2LawNext(xPPO, lastLongsX);
      lastLongsX = nextLongsX;
      final int trials = getNumTrials(nextLongsX);
      p = new Point(nextLongsX, trials);
      configure();
      //Do all trials
      p.sumTrials_nS  = 0; //total time for #trials at nextX iterations
      for (int t = 0; t < trials; t++) { //do trials
        doTrial();
      }
      println(p.getRow());
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
  private int getNumTrials(final int curX) {
    final int minBpX = 1 << lgMinBpX;
    final int maxBpX = 1 << lgMaxBpX;
    final int maxT = 1 << lgMaxT;
    final int minT = 1 << lgMinT;
    if ((lgMinT == lgMaxT) || (curX <= (minBpX))) {
      return maxT;
    }
    if (curX >= maxBpX) {
      return minT;
    }
    final double lgCurX = log(curX) / LN2;
    final double lgTrials = (slope * (lgCurX - lgMinBpX)) + lgMaxT;
    return (int) pow(2.0, lgTrials);
  }

}
