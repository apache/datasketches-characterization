/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.memory;

import static com.yahoo.sketches.Util.pwr2LawNext;
import static java.lang.Math.log;
import static java.lang.Math.pow;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import com.yahoo.memory.Memory;
import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.characterization.Properties;

/**
 * @author Lee Rhodes
 */
public abstract class BaseUtf8SpeedProfile implements JobProfile {
  Job job;
  Properties prop;
  int lgMinT;
  int lgMaxT;
  int lgMinX;
  int lgMaxX;
  int xPPO;
  int lgMinBpX; //for reducing Trials as X increases
  int lgMaxBpX;
  double slope;
  TrialStats stats;
  Point point;

  static final class TrialStats { //Data for one Trial, created once
    int[] cpArr; //created once per trial set
    double javaEncodeTime_nS; //updated every trial
    double javaDecodeTime_nS;
    double memEncodeTime_nS;
    double memDecodeTime_nS;
  }

  static class Point { //Data for a Trial Set, created only once.
    static String fmt =
          "%.2f " + TAB //LgCP/T float
        + "%d "   + TAB //CP/T int
        + "%d "   + TAB //Trials int
        + "%d "   + TAB //totCP long
        + "%.1f " + TAB //java en
        + "%.1f " + TAB //java de
        + "%.1f " + TAB //mem en
        + "%.1f";       //mem de
    int numCPPerTrial = 0;
    int trials = 0;
    double sumJavaEncodeTrials_nS = 0;
    double sumJavaDecodeTrials_nS = 0;
    double sumMemEncodeTrials_nS = 0;
    double sumMemDecodeTrials_nS = 0;

    void update(final TrialStats stats) {
      sumJavaEncodeTrials_nS += stats.javaEncodeTime_nS;
      sumJavaDecodeTrials_nS += stats.javaDecodeTime_nS;
      sumMemEncodeTrials_nS += stats.memEncodeTime_nS;
      sumMemDecodeTrials_nS += stats.memDecodeTime_nS;
    }

    void clear() {
      sumJavaEncodeTrials_nS = 0;
      sumJavaDecodeTrials_nS = 0;
      sumMemEncodeTrials_nS = 0;
      sumMemDecodeTrials_nS = 0;
    }

    static String getHeader() {
      final String s =
            "LgCP/T" + TAB
          + "CP/T" + TAB
          + "Trials" + TAB
          + "TotCP" + TAB
          + "JEnc" + TAB
          + "JDec" + TAB
          + "MEnc" + TAB
          + "MDec";
      return s;
    }

    String getRow() {
      final double lgCP = Math.log(numCPPerTrial) / LN2;
      final long totCP = trials * numCPPerTrial;
      final double meanJavaEncodePerCP_nS = sumJavaEncodeTrials_nS / totCP;
      final double meanJavaDecodePerCP_nS = sumJavaDecodeTrials_nS / totCP;
      final double meanMemEncodePerCP_nS = sumMemEncodeTrials_nS / totCP;
      final double meanMemDecodePerCP_nS = sumMemDecodeTrials_nS / totCP;

      final String out = String.format(fmt, lgCP, numCPPerTrial, trials, totCP,
          meanJavaEncodePerCP_nS, meanJavaDecodePerCP_nS,
          meanMemEncodePerCP_nS, meanMemDecodePerCP_nS);
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
    stats = new TrialStats();
    point = new Point();
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

  abstract void doTrial(TrialStats stats);

  abstract void close();

  private void doTrials() {
    println(Point.getHeader()); //GG
    final int maxX = 1 << lgMaxX;
    final int minX = 1 << lgMinX;
    int lastX = 0;
    while (lastX < maxX) { //do each plot point on the X-axis
      final int nextX = (lastX == 0) ? minX : pwr2LawNext(xPPO, lastX);
      lastX = nextX;
      final int trials = getNumTrials(nextX);
      //configure();
      point.clear();
      point.numCPPerTrial = nextX;
      point.trials = trials;
      stats.cpArr = new int[nextX];  //GG
      // Do all trials
      System.gc();
      for (int t = 0; t < trials; t++) { // do # trials
        doTrial(stats); // a single trial encode
        point.update(stats);
      }
      println(point.getRow()); //output summary of trail set at this X point //GG
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

  /**
   * Checks that expected String == actual String, and if there is an error,
   * prints out the different codepoints.
   * @param actual given
   * @param expected given
   */
  static void checkStrings(final String actual, final String expected) {
    if (!expected.equals(actual)) {
      fail("Failure: Expected (" + codepoints(expected) + ") Actual (" + codepoints(actual) + ")");
    }
  }

  static void checkMemBytes(final Memory actual, final Memory expected) {
    final long ecap = expected.getCapacity();
    final long acap = actual.getCapacity();
    final int comp = expected.compareTo(0, ecap, actual, 0, acap);
    if (comp != 0) {
      throw new IllegalArgumentException("Memory actual != Memory expected");
    }
  }

  private static List<String> codepoints(final String str) {
    final List<String> codepoints = new ArrayList<>();
    for (int i = 0; i < str.length(); i++) {
      codepoints.add(Long.toHexString(str.charAt(i)));
    }
    return codepoints;
  }

}
