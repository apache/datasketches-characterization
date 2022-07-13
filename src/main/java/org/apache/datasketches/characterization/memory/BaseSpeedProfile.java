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

package org.apache.datasketches.characterization.memory;

import static java.lang.Math.log;
import static java.lang.Math.pow;
import static org.apache.datasketches.Util.pwr2SeriesNext;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;

/**
 * @author Lee Rhodes
 */
public abstract class BaseSpeedProfile implements JobProfile {
  Job job;
  Properties prop;
  long vIn = 0;
  int lgMinT;
  int lgMaxT;
  int lgMinX;
  int lgMaxX;
  int xPPO;
  int lgMinBpX; //for reducing T
  int lgMaxBpX;
  double slope;

static class Point {
  int arrLongs;
  int trials;
  long sumReadTrials_nS = 0;
  long sumWriteTrials_nS = 0;

  Point(final int arrLongs, final int trials) {
    this.arrLongs = arrLongs;
    this.trials = trials;
  }

  public static String getHeader() {
    final String s =
          "LgLongs" + TAB
        + "Longs" + TAB
        + "Trials" + TAB
        + "#Ops" + TAB
        + "AvgRTrial_nS" + TAB
        + "AvgROp_nS" + TAB
        + "AvgWTrial_nS" + TAB
        + "AvgWOp_nS";
    return s;
  }

  public String getRow() {
    final double lgArrLongs = Math.log(arrLongs) / LN2;
    final long numOps = (long)((double)trials * arrLongs);
    final double rTrial_nS = (double)sumReadTrials_nS / trials;
    final double wTrial_nS = (double)sumWriteTrials_nS / trials;
    final double rOp_nS = rTrial_nS / arrLongs;
    final double wOp_nS = wTrial_nS / arrLongs;

    final String out = String.format("%6.2f\t%d\t%d\t%d\t%.1f\t%8.3f\t%.1f\t%8.3f",
        lgArrLongs, arrLongs, trials, numOps, rTrial_nS, rOp_nS, wTrial_nS, wOp_nS);
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
  //end JobProfile

  abstract void configure(int arrLongs);

  abstract long doTrial(final boolean read);

  abstract void close();

  private void doTrials() {
    job.println(Point.getHeader());
    final int maxX = 1 << lgMaxX;
    final int minX = 1 << lgMinX;
    int lastX = 0;
    while (lastX < maxX) {
      final int nextX = lastX == 0 ? minX : (int)pwr2SeriesNext(xPPO, lastX);
      lastX = nextX;
      final int trials = getNumTrials(nextX);
      configure(nextX);
      final Point p = new Point(nextX, trials);
      //Do all write trials first
      p.sumWriteTrials_nS = 0;
      for (int t = 0; t < trials; t++) { //do # trials
        p.sumWriteTrials_nS += doTrial(false); //a single trial write
      }
      //Do all read trials
      p.sumReadTrials_nS  = 0;
      for (int t = 0; t < trials; t++) { //do # trials
        p.sumReadTrials_nS += doTrial(true); //a single trial read
      }
      job.println(p.getRow());
    }
  }

  /**
   * Computes the number of trials for a given current number of uniques for a
   * trial set. This is used in speed trials and decreases the number of trials
   * as the number of uniques increase.
   *
   * @param curX the given current number of uniques for a trial set.
   * @return the number of trials for a given current number of uniques for a
   * trial set.
   */
  private int getNumTrials(final int curX) {
    final int minBpX = 1 << lgMinBpX;
    final int maxBpX = 1 << lgMaxBpX;
    final int maxT = 1 << lgMaxT;
    final int minT = 1 << lgMinT;
    if (lgMinT == lgMaxT || curX <= minBpX) {
      return maxT;
    }
    if (curX >= maxBpX) {
      return minT;
    }
    final double lgCurX = log(curX) / LN2;
    final double lgTrials = slope * (lgCurX - lgMinBpX) + lgMaxT;
    return (int) pow(2.0, lgTrials);
  }

}
