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
  long trialMemHash = 0;
  long trialArrHash = 0;
  long trialOldHash = 0;
  long sumHash = 0;

  static class Point {
    int longsX  ;
    int trials;
    long sumTrials_nS = 0;
    long sumTrialsFill_nS = 0;
    long sumTrialsMemHash_nS = 0;
    long sumTrialsArrHash_nS = 0;
    long sumTrialsOldHash_nS = 0;
    long sumHash = 0;

    Point(final int longsX, final int trials) {
      this.longsX = longsX;
      this.trials = trials;
    }

    public void reset(final int longsX, final int trials) {
      this.longsX = longsX;
      this.trials = trials;
      sumTrials_nS = 0;
      sumTrialsFill_nS = 0;
      sumTrialsMemHash_nS = 0;
      sumTrialsArrHash_nS = 0;
      sumTrialsOldHash_nS = 0;
    }

    public static String getHeader() {
      final String s =
            "LgLongs " + TAB
          + "Trials  " + TAB
          + "Total_mS" + TAB
          + "LFill_nS" + TAB
          + "LMemH_nS" + TAB
          + "LArrH_nS" + TAB
          + "LOldH_nS" + TAB
          + "Fill_MB/S" + TAB
          + "MemH_MB/S" + TAB
          + "ArrH_MB/S" + TAB
          + "OldH_MB/S" + TAB
          + "SumHash";
      return s;
    }

    public String getRow() {
      //final long bytesX = longsX << 3;
      final double trialsLongs = trials * longsX;
      final double lgLongsX = Math.log(longsX) / LN2;
      final double total_mS = sumTrials_nS / 1e6;
      final double avgLongFill_nS = sumTrialsFill_nS / trialsLongs;
      final double avgLongMemHash_nS = sumTrialsMemHash_nS / trialsLongs;
      final double avgLongArrHash_nS = sumTrialsArrHash_nS / trialsLongs;
      final double avgLongOldHash_nS = sumTrialsOldHash_nS / trialsLongs;
      final double fillMBPerSec = 8 / (avgLongFill_nS / 1e3);
      final double memHashMBPerSec = 8 / (avgLongMemHash_nS / 1e3);
      final double arrHashMBPerSec = 8 / (avgLongArrHash_nS / 1e3);
      final double oldHashMBPerSec = 8 / (avgLongOldHash_nS / 1e3);
      final String out = String.format(
          "%9.2f\t"  //LgBytes
        + "%9d\t"    //Trials
        + "%9.3f\t"  //Total ms
        + "%9.3f\t"  //LFill ns
        + "%9.3f\t"  //LMem ns
        + "%9.3f\t"  //LArr ns
        + "%9.3f\t"  //LOld ns
        + "%9.2f\t"  //Fill rate
        + "%9.2f\t"  //Mem rate
        + "%9.2f\t"  //Arr rate
        + "%9.2f\t"  //Old rate
        + "%16s",    //sumHash
          lgLongsX,
          trials,
          total_mS,
          avgLongFill_nS,
          avgLongMemHash_nS,
          avgLongArrHash_nS,
          avgLongOldHash_nS,
          fillMBPerSec,
          memHashMBPerSec,
          arrHashMBPerSec,
          oldHashMBPerSec,
          Long.toHexString(sumHash)
          );
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
    doPoints();
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

  private void doPoints() { //does all points
    println(Point.getHeader());
    final int maxLongsX = 1 << lgMaxX;
    final int minLongsX = 1 << lgMinX;
    int lastLongsX = 0;
    p = new Point(0, 0);
    while (lastLongsX < maxLongsX) { //
      final int nextLongsX = (lastLongsX == 0) ? minLongsX : pwr2LawNext(xPPO, lastLongsX);
      lastLongsX = nextLongsX;
      final int trials = getNumTrials(nextLongsX);
      p.reset(nextLongsX, trials);
      configure();
      //Do all trials
      p.sumTrials_nS  = 0; //total time for #trials at nextLongsX
      for (int t = 0; t < trials; t++) {
        doTrial();
        if ((trialMemHash != trialArrHash) && (trialMemHash != trialOldHash)) {
          throw new IllegalStateException("Hash checksums do not match!");
        }
        p.sumHash += trialOldHash;
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
