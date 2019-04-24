/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.fdt;

import static com.yahoo.sketches.PowerLawGenerator.getSlope;
import static com.yahoo.sketches.PowerLawGenerator.getY;
import static com.yahoo.sketches.Util.pwr2LawNext;
import static com.yahoo.sketches.characterization.PerformanceUtil.FRACTIONS;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.yahoo.sketches.DoublePair;
import com.yahoo.sketches.characterization.AccuracyStats;
import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.characterization.Properties;
import com.yahoo.sketches.fdt.FdtSketch;
import com.yahoo.sketches.fdt.Group;
import com.yahoo.sketches.fdt.PostProcessor;

/**
 * @author Lee Rhodes
 */
public class FdtAccuracyProfile implements JobProfile {
  Job job;
  PrintWriter pw;
  Properties prop;
  long vIn = 0;
  int lgK;
  int maxT;
  int lgQK;
  int lgMinU;
  int lgMaxU;
  int lgMinG;
  int lgMaxG;
  int gPPO;
  double threshold;
  double rse;
  boolean printPostProcessor;
  int numStdDev;
  int topN;
  FdtSketch sketch;
  Map<Integer,AccuracyStats> qMap;
  PostProcessor lastPost;
  int[] priKeyIndices = {0, 1, 2};
  int sketchUpdates = 0;
  int groupsGenerated = 0;
  double slope = 0;
  int minG;
  int maxG;
  int minU;
  int maxU;
  DoublePair p1;
  DoublePair p2;
  int xPoints;

  @Override
  public void start(final Job job) {
    this.job = job;
    pw = job.getPrintWriter();
    prop = job.getProperties();
    lgK = Integer.parseInt(prop.mustGet("LgK"));
    maxT = Integer.parseInt(prop.mustGet("Trials_MaxT"));
    lgQK = Integer.parseInt(prop.mustGet("Trials_lgQK"));
    lgMinU = Integer.parseInt(prop.mustGet("Trials_lgMinU"));
    lgMaxU = Integer.parseInt(prop.mustGet("Trials_lgMaxU"));
    lgMinG = Integer.parseInt(prop.mustGet("Trials_lgMinG"));
    lgMaxG = Integer.parseInt(prop.mustGet("Trials_lgMaxG"));
    gPPO = Integer.parseInt(prop.mustGet("Trials_GPPO"));
    threshold = Double.parseDouble(prop.mustGet("Threshold"));
    rse = Double.parseDouble(prop.mustGet("RSE"));
    printPostProcessor = Boolean.parseBoolean(prop.mustGet("PrintPostProcessor"));
    numStdDev = Integer.parseInt(prop.mustGet("NumStdDev"));
    topN = Integer.parseInt(prop.mustGet("TopN"));
    qMap = new HashMap<>();
    configure();
    doTrials();
    shutdown();
    cleanup();
  }

  @Override
  public void shutdown() { }

  @Override
  public void cleanup() { }

  @Override
  public void println(final String s) {
    job.println(s);
  }

  void configure() {
    if (lgK <= 0) {
      sketch = new FdtSketch(threshold, rse);
    } else {
      sketch = new FdtSketch(lgK);
    }
    minG = 1 << lgMinG;
    maxG = 1 << lgMaxG;
    minU = 1 << lgMinU;
    maxU = 1 << lgMaxU;
    p1 = new DoublePair(minG, maxU);
    p2 = new DoublePair(maxG, minU);
    slope = getSlope(p1, p2);
    xPoints = 0;
    int xG;
    for (xG = minG; xG <= maxG; xG = pwr2LawNext(gPPO, xG)) {
      xPoints++;
    }
  }

  void doTrial() {
    sketch.reset(); //reuse the same sketch
    groupsGenerated = 0;
    sketchUpdates = 0;
    int xG, yU;
    for (xG = minG; xG <= maxG; xG = pwr2LawNext(gPPO, xG)) { //select major group
      groupsGenerated += xG;
      yU = (int) Math.round(getY(p1, slope, xG)); //compute target # uniques
      for (int g = 1; g <= xG; g++) { //select the minor group
        for (int u = minU; u <= yU; u++) { //create the minor group with yU unique variations
          final String[] tuple =
            {Integer.toString(xG), Integer.toString(g), Integer.toString(yU), Long.toHexString(vIn++)};
          sketch.update(tuple);
          sketchUpdates++;
        }
      }
    }
    //sketch has been fully updated
    lastPost = sketch.getPostProcessor(new TestGroup());
    final List<Group> gpList = lastPost.getGroupList(priKeyIndices, numStdDev, 0);
    final Iterator<Group> itr = gpList.iterator();

    while (itr.hasNext()) {
      final Group gp = itr.next();
      yU = Integer.parseInt(gp.getPrimaryKey().split(",")[2]); //true uniques
      AccuracyStats q = qMap.get(yU); //get the q sketch for all priKeys with the same yU
      if (q == null) {
        q = new AccuracyStats(1 << lgQK, yU);
        q.update(gp.getEstimate());
        q.bytes = 1;
        qMap.put(yU, q);
      } else {
        q.update(gp.getEstimate());
        q.bytes++;
      }
    }
  }

  void doTrials() {
    for (int t = 0; t < maxT; t++) {
      doTrial();
    }
    //qMap contains all quantiles
    final Set<Map.Entry<Integer,AccuracyStats>> set = qMap.entrySet();
    final Iterator<Map.Entry<Integer,AccuracyStats>> itr = set.iterator();
    final List<AccuracyStats> list = new ArrayList<>();
    while (itr.hasNext()) {
      final AccuracyStats as = itr.next().getValue();
      list.add(as);
    }
    list.sort(new MyComparator());
    process(list);
  }

  private final static String hfmt =
      "%12s" + "%12s" + "%12s" + "%12s" + "%12s"
    + "%12s" + "%12s" + "%12s" + "%12s" + "%12s"
    + "%12s" + "%12s" + "%12s" + "%12s" + "%12s" + "%12s";

  private final static String fmt =
      "%12d"   + "%12.1f" + "%12.6f" + "%12.6f" + "%12d"
    + "%12.6f" + "%12.6f" + "%12.6f" + "%12.6f" + "%12.6f"
    + "%12.6f" + "%12.6f" + "%12.6f" + "%12.6f" + "%12.6f" + "%12.6f";

  private static String getHeader() {
    final String header = String.format(hfmt,
        "yU",   "MeanEst",    "MeanRelErr","RMS_RE",   "CapturedGps",
        "Min",  "Q(.0000317)","Q(.00135)", "Q(.02275)","Q(.15866)",
        "Q(.5)","Q(.84134)",  "Q(.97725)", "Q(.99865)","Q(.9999683)","Max");
    return header;
  }

  private void process(final List<AccuracyStats> list) {
    final Iterator<AccuracyStats> itr2 = list.iterator();
    job.println("Sketch: lgK: " + sketch.getLgK());
    job.println(String.format("Slope: %.2f", slope));
    job.println(getHeader());
    while (itr2.hasNext()) { //compute error for each AccuracyStats
      final AccuracyStats as = itr2.next();
      final int trials = as.bytes;
      final int uniq = (int) as.trueValue;
      final double meanEst = as.sumEst / trials;
      final double meanRelErr = as.sumRelErr / trials;
      final double meanSqErr = as.sumSqErr / trials;
      final double normMeanSqErr = meanSqErr / (1.0 * uniq * uniq);
      final double rmsRE = Math.sqrt(normMeanSqErr);
      as.rmsre = rmsRE;

      //OUTPUT
      final double[] qarr = as.qsk.getQuantiles(FRACTIONS);
      final String out = String.format(fmt,
        uniq, meanEst, meanRelErr, rmsRE, trials,
        qf(qarr[0],uniq),qf(qarr[1],uniq),qf(qarr[2],uniq),qf(qarr[3],uniq),qf(qarr[4],uniq),
        qf(qarr[5],uniq),qf(qarr[6],uniq),qf(qarr[7],uniq),qf(qarr[8],uniq),qf(qarr[9],uniq),
        qf(qarr[10],uniq));
      job.println(out);
    }
    //Print PostProcessor
    if (printPostProcessor) {
      final List<Group> gpList = lastPost.getGroupList(priKeyIndices, numStdDev, topN);
      final Iterator<Group> itr = gpList.iterator();
      job.println("");
      job.println("Data From Last Trial");
      job.println("Total Sketch Updates  : " + sketchUpdates);
      job.println("Sketch Retained Items : " + sketch.getRetainedEntries());
      job.println("Total Groups generated: " + groupsGenerated);
      job.println("Total Groups captured : " + lastPost.getGroupCount());
      job.println("Total X-axis Points   : " + xPoints);
      job.println(new TestGroup().getRowHeader());
      while (itr.hasNext()) {
        final Group gp = itr.next();
        job.println(gp.toString());
      }
    }
    println("");
    println(sketch.toString());
  }

  private static final double qf(final double q, final int u) {
    return (q / u) - 1.0;
  }



  class MyComparator implements Comparator<AccuracyStats> {
    @Override
    public int compare(final AccuracyStats o1, final AccuracyStats o2) {
      return (int) (o2.trueValue - o1.trueValue);
    }
  }

}
