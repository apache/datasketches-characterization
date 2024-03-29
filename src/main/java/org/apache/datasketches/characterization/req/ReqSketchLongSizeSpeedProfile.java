/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.characterization.req;

import static java.lang.Math.log;
import static java.lang.Math.pow;
//import static org.apache.datasketches.Util.pwr2LawNext;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;
import org.apache.datasketches.req.ReqSketch;
import org.apache.datasketches.req.ReqSketchBuilder;

/**
 * @author Lee Rhodes
 * @author Pavel Vesely
 */
@SuppressWarnings("unused")
public class ReqSketchLongSizeSpeedProfile implements JobProfile {
  private Job job;
  private Properties prop;

  //FROM PROPERTIES
  private int lgMinT;
  private int lgMaxT;
  private int lgMinBpSL;
  private int lgMaxBpSL;
  //For computing the different stream lengths
  private int lgMinSL;
  private int lgMaxSL;

  private double slope;

  //Target sketch configuration & error analysis
  private int reqK;
  private boolean hra; //high rank accuracy
  private boolean ltEq;

  //DERIVED & GLOBALS
  private ReqSketch reqSk;
  //private KllFloatsSketch kllSk;

  private final String[] columnLabels = {"PP", "SL", "Trials", "ReqBytes", "nS/u" };
  private final String sFmt =  "%2s\t%2s\t%6s\t%8s\t%4s\n";
  private final String dFmt =  "%,6d\t%,12d\t%,12d\t%,12d\t%,12.6f\n";

  private void extractProperties() {
    //trials config
    lgMinT = Integer.parseInt(prop.mustGet("LgMinT"));
    lgMaxT = Integer.parseInt(prop.mustGet("LgMaxT"));
    lgMinBpSL = Integer.parseInt(prop.mustGet("LgMinBpSL"));
    lgMaxBpSL = Integer.parseInt(prop.mustGet("LgMaxBpSL"));
    //stream length
    lgMinSL = Integer.parseInt(prop.mustGet("LgMinSL"));
    lgMaxSL = Integer.parseInt(prop.mustGet("LgMaxSL"));

    //Target sketch config
    reqK = Integer.parseInt(prop.mustGet("ReqK"));
    hra = Boolean.parseBoolean(prop.mustGet("HRA"));
    ltEq = prop.mustGet("Criterion").equals("LE") ? true : false;

  }

  void configureCommon() {
    slope = (double) (lgMaxT - lgMinT) / (lgMinBpSL - lgMaxBpSL);
  }

  void configureSketch() {
    final ReqSketchBuilder bldr = ReqSketch.builder();
    bldr.setK(reqK).setHighRankAccuracy(hra);
    reqSk = bldr.build();
  }

//JobProfile interface
  @Override
  public void start(final Job job) {
    this.job = job;
    prop = job.getProperties();
    extractProperties();
    configureCommon();
    configureSketch();
    doTrials();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}
  //end JobProfile

  /**
   * Traverses all the axis plot points and performs trials(sl) at each point
   * and outputs a row per axis plot point.
   */
  private void doTrials() {
    final long maxSL = 1L << lgMaxSL;
    final long minSL = 1L << lgMinSL;
    long lastSL = 0;
    job.printf(sFmt, (Object[]) columnLabels); //Header
    int pp = 1;
    while (lastSL < maxSL) { //Trials for each plotPoint on X-axis, and one row on output
      final long nextSL = lastSL == 0 ? minSL : 2 * lastSL;
      lastSL = nextSL;
      final int trials = getNumTrials(nextSL);

      double sumUpdateTimePerItem_nS = 0;
      for (int t = 0; t < trials; t++) {
        sumUpdateTimePerItem_nS += doTrial(nextSL);
      }
      final double meanUpdateTimePerItem_nS = sumUpdateTimePerItem_nS / trials;
      final int bytes = reqSk.getSerializedSizeBytes();
      job.printf(dFmt, pp, nextSL, trials, bytes, meanUpdateTimePerItem_nS);
      pp++;
    }
  }

  /**
   * Return the average update time per item for this trial
   * @param streamLen the streamLength for this trial
   * @return the average update time per item for this trial
   */
  private double doTrial(final long streamLen) {
    reqSk.reset();
    final long startUpdateTime_nS = System.nanoTime();

    for (long i = 0; i < streamLen; i++) {
      reqSk.update(i);
    }
    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return (double) updateTime_nS / streamLen;
  }

  /**
   * Computes the number of trials for a given current stream length for a
   * trial set. This is used in speed trials and decreases the number of trials
   * as the stream length increases.
   *
   * @param curSL the given current stream length for a trial set.
   * @return the number of trials for a given current stream length for a
   * trial set.
   */
  private int getNumTrials(final long curSL) {
    final int minBpSL = 1 << lgMinBpSL;
    final int maxBpSL = 1 << lgMaxBpSL;
    final int maxT = 1 << lgMaxT;
    final int minT = 1 << lgMinT;
    if (lgMinT == lgMaxT || curSL <= minBpSL) {
      return maxT;
    }
    if (curSL >= maxBpSL) {
      return minT;
    }
    final double lgCurU = log(curSL) / LN2;
    final double lgTrials = slope * (lgCurU - lgMinBpSL) + lgMaxT;
    return (int) pow(2.0, lgTrials);
  }

}
