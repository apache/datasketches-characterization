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

package org.apache.datasketches.characterization.uniquecount;

//import static java.lang.Math.log;
//import static java.lang.Math.pow;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;

/**
 * @author Lee Rhodes
 */
//@SuppressWarnings("javadoc")
public abstract class BaseMergeSpeedProfile implements JobProfile  {
  Job job;
  public Properties prop;
  public long vIn = 0;
  int minLgT;
  int maxLgT;
  int minLgK;
  int maxLgK;
  public int lgDeltaU;
  public boolean serDe;

  public Stats stats = new Stats();

  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
    prop = job.getProperties();
    minLgT = Integer.parseInt(prop.mustGet("MinLgT"));
    maxLgT = Integer.parseInt(prop.mustGet("MaxLgT"));
    minLgK = Integer.parseInt(prop.mustGet("MinLgK"));
    maxLgK = Integer.parseInt(prop.mustGet("MaxLgK"));
    lgDeltaU = Integer.parseInt(prop.mustGet("LgDeltaU"));
    serDe = Boolean.parseBoolean(prop.mustGet("SerDe"));
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

  /**
   * Configure the sketch
   */
  public abstract void configure();

  /**
   * Perform a single trial
   * @param stats stats array
   * @param lgK sketch size
   * @param lgDeltaU delta size determining U: +1 = 2K, +2 = 4K; -1 = K/2, -2 = K/4, etc.
   */
  public abstract void doTrial(Stats stats, int lgK, int lgDeltaU);

  public abstract void resetMerge(int lgK);

  private void doTrials() {
    final StringBuilder dataStr = new StringBuilder();
    job.println(getHeader());
    final Stats stats = new Stats();
    int lgK;

    for (lgK = minLgK; lgK <= maxLgK; lgK++) {
      final int lgT = maxLgK - lgK + minLgT;
      final int trials = 1 << lgT;
      double sumSerializeTime_nS = 0;
      double sumDeserialzeTime_nS = 0;
      double sumMergeTime_nS = 0;
      double sumTotalTime_nS = 0;
      resetMerge(lgK);
      for (int t = 0; t < trials; t++) {
        doTrial(stats, lgK, lgDeltaU);
        sumSerializeTime_nS += stats.serializeTime_nS;
        sumDeserialzeTime_nS += stats.deserializeTime_nS;
        sumMergeTime_nS += stats.mergeTime_nS;
        sumTotalTime_nS += stats.totalTime_nS;
      }
      //Per sketch per trial
      stats.serializeTime_nS = sumSerializeTime_nS / trials;
      stats.deserializeTime_nS = sumDeserialzeTime_nS / trials;
      stats.mergeTime_nS = sumMergeTime_nS / trials;
      stats.totalTime_nS = sumTotalTime_nS / trials;
      process(stats, lgK, lgT, dataStr);
      job.println(dataStr.toString());
    }
  }

  private static void process(final Stats stats,
      final int lgK, final int lgT, final StringBuilder dataStr) {

    //OUTPUT
    dataStr.setLength(0);
    dataStr.append(lgK).append(TAB);
    dataStr.append(lgT).append(TAB);
    dataStr.append(stats.serializeTime_nS).append(TAB);
    dataStr.append(stats.deserializeTime_nS).append(TAB);
    dataStr.append(stats.mergeTime_nS).append(TAB);
    dataStr.append(stats.totalTime_nS).append(TAB);
    final double slotTime_nS = stats.totalTime_nS / (1 << lgK);
    dataStr.append(slotTime_nS);
  }

  private static String getHeader() {
    final StringBuilder sb = new StringBuilder();
    sb.append("LgK").append(TAB);
    sb.append("LgT").append(TAB);
    sb.append("Ser_nS").append(TAB);
    sb.append("DeSer_nS").append(TAB);
    sb.append("Merge_nS").append(TAB);
    sb.append("Total_nS").append(TAB);
    sb.append("PerSlot_nS");
    return sb.toString();
  }

  public static class Stats {
    public double serializeTime_nS;
    public double deserializeTime_nS;
    public double mergeTime_nS;
    public double totalTime_nS;
  }
}
