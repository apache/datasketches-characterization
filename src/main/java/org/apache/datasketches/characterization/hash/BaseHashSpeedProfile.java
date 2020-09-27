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

package org.apache.datasketches.characterization.hash;

import static java.lang.Math.log;
import static java.lang.Math.pow;
import static org.apache.datasketches.Util.pwr2LawNext;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;

/**
 * @author Lee Rhodes
 */
public abstract class BaseHashSpeedProfile implements JobProfile {
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
  BasePoint p;

  abstract class BasePoint {
    int x;
    int trials;
    long sumTrials_nS;

    BasePoint(final int x, final int trials) {
      this.x = x;
      this.trials = trials;
      sumTrials_nS = 0;
    }

    public abstract void reset(final int x, final int trials);

    public abstract String getHeader();

    public abstract String getRow();
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
  //end JobProfile

  abstract void configure();

  abstract void doTrial();

  abstract void close();

  void doPoints() { //does all points
    job.println(p.getHeader());
    final int maxX = 1 << lgMaxX;
    final int minX = 1 << lgMinX;
    int lastX = 0;
    while (lastX < maxX) {
      final int nextX = lastX == 0 ? minX : pwr2LawNext(xPPO, lastX);
      lastX = nextX;
      final int trials = getNumTrials(nextX);
      p.reset(nextX, trials);
      configure();

      //Do all trials
      p.sumTrials_nS  = 0; //total time for #trials at nextX
      for (int t = 0; t < trials; t++) {
        doTrial();
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
