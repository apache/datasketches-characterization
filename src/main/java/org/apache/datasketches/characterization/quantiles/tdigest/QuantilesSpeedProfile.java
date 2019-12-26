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

package org.apache.datasketches.characterization.quantiles.tdigest;

import static org.apache.datasketches.Util.pwr2LawNext;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;

public abstract class QuantilesSpeedProfile implements JobProfile {

  private Job job;

  @Override
  public void start(final Job job) {
    this.job = job;
    doTrials();
  }

  @Override
  public void println(final Object o) {
    job.println(o.toString());
  }

  private void doTrials() {
    final int lgMinStreamLen = Integer.parseInt(job.getProperties().mustGet("lgMin"));
    final int lgMaxStreamLen = Integer.parseInt(job.getProperties().mustGet("lgMax"));
    final int minStreamLen = 1 << lgMinStreamLen;
    final int maxStreamLen = 1 << lgMaxStreamLen;
    final int pointsPerOctave = Integer.parseInt(job.getProperties().mustGet("PPO"));

    final int lgMaxTrials = Integer.parseInt(job.getProperties().mustGet("lgMaxTrials"));
    final int lgMinTrials = Integer.parseInt(job.getProperties().mustGet("lgMinTrials"));

    final int k = Integer.parseInt(job.getProperties().mustGet("K"));
    final int numQueryValues = Integer.parseInt(job.getProperties().mustGet("numQueryValues"));

    configure(k, numQueryValues, job.getProperties());

    println(getHeader());

    int streamLength = minStreamLen;
    while (streamLength <= maxStreamLen) {
      prepareTrial(streamLength);
      final int numTrials = getNumTrials(streamLength, lgMinStreamLen, lgMaxStreamLen,
          lgMinTrials, lgMaxTrials);
      for (int i = 0; i < numTrials; i++) {
        doTrial();
      }
      println(getStats(streamLength, numTrials, numQueryValues));
      streamLength = pwr2LawNext(pointsPerOctave, streamLength);
    }
  }

  abstract void configure(int k, int numQueryValues, Properties properties);

  abstract void prepareTrial(int streamLength);

  abstract void doTrial();

  abstract String getHeader();

  abstract String getStats(int streamLength, int numTrials, int numQueryValues);

  private static int getNumTrials(final int x, final int lgMinX, final int lgMaxX,
      final int lgMinTrials, final int lgMaxTrials) {
    final double slope = (double) (lgMaxTrials - lgMinTrials) / (lgMinX - lgMaxX);
    final double lgX = Math.log(x) / JobProfile.LN2;
    final double lgTrials = (slope * lgX) + lgMaxTrials;
    return (int) Math.pow(2, lgTrials);
  }

}
