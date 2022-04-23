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

package org.apache.datasketches.characterization.kll;

import static java.lang.Math.log;
import static java.lang.Math.pow;
import static org.apache.datasketches.Util.pwr2LawNext;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class KllSketchSizeSpeedProfile implements JobProfile {
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();
  private Job job;
  private Properties prop;

  //FROM PROPERTIES
  private int lgMinT;
  private int lgMaxT;
  private int lgMinBpSL;
  private int lgMaxBpSL;
  private String type;
  //For computing the different stream lengths
  private int lgMinSL;
  private int lgMaxSL;
  private int ppoSL;

  private double slope;

  //Target sketch configuration & error analysis
  private int k;
  private boolean useDouble = false;
  private boolean direct = false;

  //DERIVED & GLOBALS
  private KllDoublesSketch dsk = null;
  private KllFloatsSketch fsk = null;

  private final String[] columnLabels = {"PP", "SL", "Trials", "KllBytes", "Kll nS" };
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
    ppoSL = Integer.parseInt(prop.mustGet("PpoSL"));

    //Target sketch config
    k = Integer.parseInt(prop.mustGet("KllK"));
    type = prop.mustGet("type");
    if (type.equalsIgnoreCase("double")) { useDouble = true; }
    direct = Boolean.parseBoolean(prop.mustGet("direct"));
  }

  void configureCommon() {
    slope = (double) (lgMaxT - lgMinT) / (lgMinBpSL - lgMaxBpSL);

  }

  void configureSketch() {
    if (useDouble) {
      if (direct) {
        final WritableMemory dstMem = WritableMemory.allocate(10000);
        dsk = KllDoublesSketch.newDirectInstance(k, dstMem, memReqSvr);
      } else { //heap
        dsk = KllDoublesSketch.newHeapInstance(k);
      }
    } else { //useFloat
      if (direct) {
        final WritableMemory dstMem = WritableMemory.allocate(10000);
        fsk = KllFloatsSketch.newDirectInstance(k, dstMem, memReqSvr);
      } else { //heap
        fsk = KllFloatsSketch.newHeapInstance(k);
      }
    }
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
    final int maxSL = 1 << lgMaxSL;
    final int minSL = 1 << lgMinSL;
    int lastSL = 0;
    job.printf(sFmt, (Object[]) columnLabels); //Header
    int pp = 1;
    while (lastSL < maxSL) { //Trials for each plotPoint on X-axis, and one row on output
      final int nextSL = lastSL == 0 ? minSL : pwr2LawNext(ppoSL, lastSL);
      lastSL = nextSL;
      final int trials = getNumTrials(nextSL);

      double sumUpdateTimePerItem_nS = 0;
      for (int t = 0; t < trials; t++) {
        sumUpdateTimePerItem_nS += doTrial(nextSL);
      }
      final double meanUpdateTimePerItem_nS = sumUpdateTimePerItem_nS / trials;
      final int bytes = useDouble ? dsk.getSerializedSizeBytes() : fsk.getSerializedSizeBytes();
      job.printf(dFmt, pp, nextSL, trials, bytes, meanUpdateTimePerItem_nS);
      pp++;
    }
  }

  /**
   * Return the average update time per item for this trial
   * @param streamLen the streamLength for this trial
   * @return the average update time per item for this trial
   */
  private double doTrial(final int streamLen) {
    if (useDouble) {
      dsk.reset();
      final long startUpdateTime_nS = System.nanoTime();

      for (int i = 0; i < streamLen; i++) {
        dsk.update(i);
      }
      final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
      return (double) updateTime_nS / streamLen;
    }
    else { //use Float
      fsk.reset();
      final long startUpdateTime_nS = System.nanoTime();

      for (int i = 0; i < streamLen; i++) {
        fsk.update(i);
      }
      final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
      return (double) updateTime_nS / streamLen;
    }
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
  private int getNumTrials(final int curSL) {
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
