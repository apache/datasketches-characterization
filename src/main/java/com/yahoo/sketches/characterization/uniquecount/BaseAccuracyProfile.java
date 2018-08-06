/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

import static com.yahoo.sketches.Util.milliSecToString;
import static com.yahoo.sketches.Util.pwr2LawNext;
import static com.yahoo.sketches.characterization.PerformanceUtil.FRACTIONS;
import static com.yahoo.sketches.characterization.PerformanceUtil.FRACT_LEN;

import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.characterization.PerformanceUtil;
import com.yahoo.sketches.characterization.Properties;
import com.yahoo.sketches.quantiles.DoublesSketch;

/**
 * @author Lee Rhodes
 */
public abstract class BaseAccuracyProfile implements JobProfile {
  Job job;
  Properties prop;
  long vIn = 0;
  int lgMinT;
  int lgMaxT;
  int tPPO;
  int lgMinU;
  int lgMaxU;
  int uPPO;
  int lgQK;
  int lgK;
  boolean interData;
  boolean postPMFs;
  boolean getSize = false;
  AccuracyStats[] qArr;

  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
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
    qArr = buildAccuracyStatsArray(prop, this);
    lgK = Integer.parseInt(prop.mustGet("LgK"));
    final String getSizeStr = prop.get("Trials_bytes");
    getSize = (getSizeStr == null) ? false : Boolean.parseBoolean(getSizeStr);
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

  abstract void configure();

  /**
   * An accuracy trial is one pass through all uniques, pausing to store the estimate into a
   * quantiles sketch at each point along the unique axis.
   */
  abstract void doTrial();

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
   *
   * @param job the given job
   * @param profile the given profile
   */
  private void doTrials() {
    final int minT = 1 << lgMinT;
    final int maxT = 1 << lgMaxT;
    final int maxU = 1 << lgMaxU;

    //This will generate a table of data up for each intermediate Trials point
    int lastT = 0;
    while (lastT < maxT) {
      final int nextT = (lastT == 0) ? minT : pwr2LawNext(tPPO, lastT);
      final int delta = nextT - lastT;
      for (int i = 0; i < delta; i++) {
        doTrial();
      }
      lastT = nextT;
      final StringBuilder sb = new StringBuilder();
      if (nextT < maxT) { // intermediate
        if (interData) {
          job.println(getHeader());
          process(getSize, qArr, lastT, sb);
          job.println(sb.toString());
        }
      } else { //done
        job.println(getHeader());
        process(getSize, qArr, lastT, sb);
        job.println(sb.toString());
      }

      println(prop.extractKvPairs());
      println("Cum Trials             : " + lastT);
      println("Cum Updates            : " + vIn);
      final long currentTime_mS = System.currentTimeMillis();
      final long cumTime_mS = currentTime_mS - job.getStartTime();
      println("Cum Trials Time        : " + milliSecToString(cumTime_mS));
      final double timePerTrial_mS = (cumTime_mS * 1.0) / lastT;
      final double avgUpdateTime_ns = (timePerTrial_mS * 1e6) / maxU;
      println("Time Per Trial, mSec   : " + timePerTrial_mS);
      println("Avg Update Time, nSec  : " + avgUpdateTime_ns);
      println("Date Time              : "
          + job.getReadableDateString(currentTime_mS));

      final long timeToComplete_mS = (long)(timePerTrial_mS * (maxT - lastT));
      println("Est Time to Complete   : " + milliSecToString(timeToComplete_mS));
      println("Est Time at Completion : "
          + job.getReadableDateString(timeToComplete_mS + currentTime_mS));
      println("");
      if (postPMFs) {
        for (int i = 0; i < qArr.length; i++) {
          println(outputPMF(qArr[i]));
        }
      }
    }
  }

  private static void process(final boolean getSize, final AccuracyStats[] qArr,
      final int cumTrials, final StringBuilder sb) {

    final int points = qArr.length;
    sb.setLength(0);
    for (int pt = 0; pt < points; pt++) {
      final AccuracyStats q = qArr[pt];
      final double uniques = q.trueValue;
      final double meanEst = q.sumEst / cumTrials;
      final double meanRelErr = q.sumRelErr / cumTrials;
      final double meanSqErr = q.sumSqErr / cumTrials;
      final double normMeanSqErr = meanSqErr / (1.0 * uniques * uniques);
      final double rmsRelErr = Math.sqrt(normMeanSqErr);
      q.rmsre = rmsRelErr;
      final int bytes = q.bytes;

      //OUTPUT
      //sb.setLength(0);
      sb.append(uniques).append(TAB);

      //Sketch meanEst, meanEstErr, norm RMS Err
      sb.append(meanEst).append(TAB);
      sb.append(meanRelErr).append(TAB);
      sb.append(rmsRelErr).append(TAB);

      //TRIALS
      sb.append(cumTrials).append(TAB);

      //Quantiles
      final double[] quants = qArr[pt].qsk.getQuantiles(FRACTIONS);
      for (int i = 0; i < FRACT_LEN; i++) {
        sb.append((quants[i] / uniques) - 1.0).append(TAB);
      }
      if (getSize) {
        sb.append(bytes).append(TAB);
        sb.append(rmsRelErr * Math.sqrt(bytes));
      } else {
        sb.append(0).append(TAB);
        sb.append(0);
      }
      sb.append(LS);
    }
  }

  private static String getHeader() {
    final StringBuilder sb = new StringBuilder();
    sb.append("InU").append(TAB);        //col 1
    //Estimates
    sb.append("MeanEst").append(TAB);    //col 2
    sb.append("MeanRelErr").append(TAB); //col 3
    sb.append("RMS_RE").append(TAB);     //col 4

    //Trials
    sb.append("Trials").append(TAB);     //col 5

    //Quantiles
    sb.append("Min").append(TAB);
    sb.append("Q(.0000317)").append(TAB);
    sb.append("Q(.00135)").append(TAB);
    sb.append("Q(.02275)").append(TAB);
    sb.append("Q(.15866)").append(TAB);
    sb.append("Q(.5)").append(TAB);
    sb.append("Q(.84134)").append(TAB);
    sb.append("Q(.97725)").append(TAB);
    sb.append("Q(.99865)").append(TAB);
    sb.append("Q(.9999683)").append(TAB);
    sb.append("Max").append(TAB);
    sb.append("Bytes").append(TAB);
    sb.append("ReMerit");
    return sb.toString();
  }

  /**
   *
   * @param prop the given Properties
   * @return an AccuracyStats array
   */
  private static final AccuracyStats[] buildAccuracyStatsArray(final Properties prop,
      final BaseAccuracyProfile profile) {
    final int lgMinU = profile.lgMinU;
    final int lgMaxU = profile.lgMaxU;
    final int uPPO = profile.uPPO;
    final int lgQK = profile.lgQK;

    final int qLen = PerformanceUtil.countPoints(lgMinU, lgMaxU, uPPO);
    final AccuracyStats[] qArr = new AccuracyStats[qLen];
    int p = 1 << lgMinU;
    for (int i = 0; i < qLen; i++) {
      qArr[i] = new AccuracyStats(1 << lgQK, p);
      p = pwr2LawNext(uPPO, p);
    }
    return qArr;
  }

  /**
   * Outputs the Probability Mass Function given the AccuracyStats.
   * @param q the given AccuracyStats
   */
  private static String outputPMF(final AccuracyStats q) {
    final DoublesSketch qSk = q.qsk;
    final double[] splitPoints = qSk.getQuantiles(FRACTIONS); //1:1
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
