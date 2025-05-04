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

import static org.apache.datasketches.GaussianRanks.GAUSSIANS_3SD;
import static org.apache.datasketches.common.Util.milliSecToString;
import static org.apache.datasketches.common.Util.pwr2SeriesNext;

import java.io.PrintWriter;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.BoundsAccuracyStats;
import org.apache.datasketches.quantiles.DoublesSketch;

/**
 * @author Lee Rhodes
 */
public abstract class BaseBoundsAccuracyProfile implements JobProfile {
  Job job;
  PrintWriter pw;
  public Properties prop;
  public long vIn = 0;
  int lgMinT;
  int lgMaxT;
  int tPPO;
  int lgMinU;
  int lgMaxU;
  int uPPO;
  int lgQK;
  public int lgK;
  boolean interData;
  boolean postPMFs;
  public boolean getSize = false;
  public BoundsAccuracyStats[] qArr;

  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
    pw = job.getPrintWriter();
    prop = job.getProperties();
    lgMinT = Integer.parseInt(prop.mustGet("Trials_lgMinT"));
    lgMaxT = Integer.parseInt(prop.mustGet("Trials_lgMaxT"));
    tPPO = Integer.parseInt(prop.mustGet("Trials_TPPO"));
    lgMinU = Integer.parseInt(prop.mustGet("Trials_lgMinU"));
    lgMaxU = Integer.parseInt(prop.mustGet("Trials_lgMaxU"));
    interData = Boolean.parseBoolean(prop.mustGet("Trials_interData"));
    postPMFs = Boolean.parseBoolean(prop.mustGet("Trials_postPMFs"));
    uPPO = Integer.parseInt(prop.mustGet("Trials_UPPO"));
    lgQK = Integer.parseInt(prop.mustGet("Trials_lgQK"));
    qArr = BoundsAccuracyStats.buildAccuracyStatsArray(lgMinU, lgMaxU, uPPO, lgQK);
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

  public abstract void configure();

  /**
   * An accuracy trial is one pass through all uniques, pausing to store the estimate into a
   * quantiles sketch at each point along the unique axis.
   */
  public abstract void doTrial();

  /**
   * Manages multiple trials for measuring accuracy.
   *
   * <p>An accuracy trial is run along the count axis (X-axis) first. The "points" along the X-axis
   * where accuracy data is collected is controlled by the data loaded into the CountAccuracyStats
   * array. A single trial consists of a single sketch being updated with the Trials_lgMaxU unique
   * values, stopping at the configured x-axis points along the way where the accuracy is recorded
   * into the corresponding stats array. Each stats array retains the distribution of
   * the accuracies measured for all the trials at that x-axis point.
   *
   * <p>Because accuracy trials take a long time, this profile will output intermediate
   * accuracy results starting after Trials_lgMinT trials and then again at trial intervals
   * determined by Trials_TPPO until Trials_lgMaxT.  This allows you to stop the testing at
   * any intermediate trials point if you feel you have sufficient trials for the accuracy you
   * need.
   */
  private void doTrials() {
    final int minT = 1 << lgMinT;
    final int maxT = 1 << lgMaxT;
    final int maxU = 1 << lgMaxU;

    //This will generate a table of data up for each intermediate Trials point
    int lastT = 0;
    while (lastT < maxT) {
      final int nextT = lastT == 0 ? minT : (int)pwr2SeriesNext(tPPO, lastT);
      final int delta = nextT - lastT;
      for (int i = 0; i < delta; i++) {
        doTrial();
      }
      lastT = nextT;
      final StringBuilder sb = new StringBuilder();
      if (nextT < maxT) { // intermediate
        if (interData) {
          job.println(getHeader());
          process(qArr, lastT, sb);
          job.println(sb.toString());
        }
      } else { //done
        job.println(getHeader());
        process(qArr, lastT, sb);
        job.println(sb.toString());
      }

      job.println(prop.extractKvPairs());
      job.println("Cum Trials             : " + lastT);
      job.println("Cum Updates            : " + vIn);
      final long currentTime_mS = System.currentTimeMillis();
      final long cumTime_mS = currentTime_mS - job.getStartTime();
      job.println("Cum Time               : " + milliSecToString(cumTime_mS));
      final double timePerTrial_mS = cumTime_mS * 1.0 / lastT;
      final double avgUpdateTime_ns = timePerTrial_mS * 1e6 / maxU;
      job.println("Time Per Trial, mSec   : " + timePerTrial_mS);
      job.println("Avg Update Time, nSec  : " + avgUpdateTime_ns);
      job.println("Date Time              : "
          + job.getReadableDateString(currentTime_mS));

      final long timeToComplete_mS = (long)(timePerTrial_mS * (maxT - lastT));
      job.println("Est Time to Complete   : " + milliSecToString(timeToComplete_mS));
      job.println("Est Time at Completion : "
          + job.getReadableDateString(timeToComplete_mS + currentTime_mS));
      job.println("");
      if (postPMFs) {
        for (int i = 0; i < qArr.length; i++) {
          job.println(outputPMF(qArr[i]));
        }
      }
      job.flush();
    }
  }

  private static void process(final BoundsAccuracyStats[] qArr,
      final int cumTrials, final StringBuilder sb) {

    final int points = qArr.length;
    sb.setLength(0);
    for (int pt = 0; pt < points; pt++) {
      final BoundsAccuracyStats q = qArr[pt];
      final double uniques = q.trueValue;
      final double relLb3 = q.sumLB3 / cumTrials / uniques - 1.0;
      final double relLb2 = q.sumLB2 / cumTrials / uniques - 1.0;
      final double relLb1 = q.sumLB1 / cumTrials / uniques - 1.0;
      final double relUb1 = q.sumUB1 / cumTrials / uniques - 1.0;
      final double relUb2 = q.sumUB2 / cumTrials / uniques - 1.0;
      final double relUb3 = q.sumUB3 / cumTrials / uniques - 1.0;

      //OUTPUT
      sb.append(uniques).append(TAB);
      //TRIALS
      sb.append(cumTrials).append(TAB);

      //Quantiles
      final double[] quants = q.qskEst.getQuantiles(GAUSSIANS_3SD);
      final int len = quants.length;
      for (int i = 0; i < len; i++) {
        sb.append(quants[i] / uniques - 1.0).append(TAB);
      }
      //Bound averages
      sb.append(relLb3).append(TAB);
      sb.append(relLb2).append(TAB);
      sb.append(relLb1).append(TAB);
      sb.append(relUb1).append(TAB);
      sb.append(relUb2).append(TAB);
      sb.append(relUb3).append(TAB);
      sb.append(LS);
    }
  }

  private static String getHeader() {
    final StringBuilder sb = new StringBuilder();
    sb.append("InU").append(TAB);
    //Trials
    sb.append("Trials").append(TAB);
    //Quantiles
    sb.append("Min").append(TAB);
    sb.append("Q(.00135)").append(TAB);
    sb.append("Q(.02275)").append(TAB);
    sb.append("Q(.15866)").append(TAB);
    sb.append("Q(.5)").append(TAB);
    sb.append("Q(.84134)").append(TAB);
    sb.append("Q(.97725)").append(TAB);
    sb.append("Q(.99865)").append(TAB);
    sb.append("Max").append(TAB);
    sb.append("avgLB3").append(TAB);
    sb.append("avgLB2").append(TAB);
    sb.append("avgLB1").append(TAB);
    sb.append("avgUB1").append(TAB);
    sb.append("avgUB2").append(TAB);
    sb.append("avgUB3").append(TAB);
    return sb.toString();
  }

  /**
   * Outputs the Probability Mass Function given the AccuracyStats.
   * @param q the given AccuracyStats
   */
  private static String outputPMF(final BoundsAccuracyStats q) {
    final DoublesSketch qSk = q.qskEst;
    final double[] splitPoints = qSk.getQuantiles(GAUSSIANS_3SD); //1:1
    final double[] reducedSp = reduceSplitPoints(splitPoints);
    final double[] pmfArr = qSk.getPMF(reducedSp); //pmfArr is one larger
    final long trials = qSk.getN();
    final StringBuilder sb = new StringBuilder();

    //output Histogram
    final String hdr = String.format("%10s%4s%12s", "Trials", "    ", "Est");
    final String fmt = "%10d%4s%12.2f";
    sb.append("Histogram At " + q.trueValue).append(LS);
    sb.append(hdr).append(LS);
    for (int i = 0; i < reducedSp.length; i++) {
      final int hits = (int)(pmfArr[i + 1] * trials);
      final double est = reducedSp[i];
      final String line = String.format(fmt, hits, " >= ", est);
      sb.append(line).append(LS);
    }
    return sb.toString();
  }

  /**
   *
   * @param splitPoints the given splitPoints
   * @return the reduced array of splitPoints
   */
  private static double[] reduceSplitPoints(final double[] splitPoints) {
    int num = 1;
    double lastV = splitPoints[0];
    for (int i = 0; i < splitPoints.length; i++) {
      final double v = splitPoints[i];
      if (v <= lastV) { continue; }
      num++;
      lastV = v;
    }
    lastV = splitPoints[0];
    int idx = 0;
    final double[] sp = new double[num];
    sp[0] = lastV;
    for (int i = 0; i < splitPoints.length; i++) {
      final double v = splitPoints[i];
      if (v <= lastV) { continue; }
      sp[++idx] = v;
      lastV = v;
    }
    return sp;
  }

}
