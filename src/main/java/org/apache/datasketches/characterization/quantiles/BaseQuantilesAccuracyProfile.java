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

package org.apache.datasketches.characterization.quantiles;

import static org.apache.datasketches.GaussianRanks.M1SD;
import static org.apache.datasketches.GaussianRanks.M2SD;
import static org.apache.datasketches.GaussianRanks.M3SD;
import static org.apache.datasketches.GaussianRanks.MED;
import static org.apache.datasketches.GaussianRanks.P1SD;
import static org.apache.datasketches.GaussianRanks.P2SD;
import static org.apache.datasketches.GaussianRanks.P3SD;
import static org.apache.datasketches.Util.pwr2LawNext;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.MonotonicPoints;
import org.apache.datasketches.Properties;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

/**
 * For capturing the maximum error over T trials a single quantile error threshold.
 *
 * <p>Performs the following:</p>
 * <ul>
 * <li>Starts the Job</li>
 * <li>Computes the number of X-axis plot points based on lgMin,lgMax, PPO.</li>
 * <li>Configures the array of error capturing quantile sketches of size errLgK</li>
 * <li>Computes the single error threshold errPct</li>
 * <li>Runs doTrials(), which loops through all plot points and at each plot point
 * executes doTrial() "trials" times.</li>
 * <li>The doTrial() is executed by the child class</li>
 * <li>The doTrial() returns the maximum error over all points in the stream.</li>
 * <li>Load the maximum rank error from each trial into the error quantiles sketch.</li>
 * <li>This prints out the "errPct" error over all trials for each specific stream length (SL).</li>
 * </ul>
 *
 * @author lrhodes
 *
 */
public abstract class BaseQuantilesAccuracyProfile implements JobProfile {

  protected Job job;
  protected Properties props;

  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
    props = job.getProperties();
    doTrials();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}
  //end JobProfile

  private static final double[] G_QUANTILES_3SD = {M3SD, M2SD, M1SD, MED, P1SD, P2SD, P3SD};

  private void doTrials() {
    final int lgMin = Integer.parseInt(props.mustGet("lgMin"));
    final int lgMax = Integer.parseInt(props.mustGet("lgMax"));
    final int ppo = Integer.parseInt(props.mustGet("PPO"));
    final int numTrials = Integer.parseInt(props.mustGet("trials"));
    final int errorSketchLgK = Integer.parseInt(props.mustGet("errLgK"));

    final DoublesSketchBuilder builder = DoublesSketch.builder().setK(1 << errorSketchLgK);

    configure();
    job.println("Epsilon:\t" + getEpsilon());

    //PRINT HEADER
    job.println("StreamLength\t-3SD\t-2SD\t-1SD\tMEDIAN\t+1SD\t+2SD\t+3SD");

    final int numSteps = MonotonicPoints.countPoints(lgMin, lgMax, ppo);
    int streamLength = 1 << lgMin;
    for (int i = 0; i < numSteps; i++) {
      prepareTrialSet(streamLength);
      final UpdateDoublesSketch rankErrorSketch = builder.build();
      for (int t = 0; t < numTrials; t++) {
        final double worstRankErrorInTrial = doTrial();
        rankErrorSketch.update(worstRankErrorInTrial);
      }
      final double[] qArr = rankErrorSketch.getQuantiles(G_QUANTILES_3SD);

      job.println(streamLength + "\t"
          + String.format("%.16f\t%.16f\t%.16f\t%.16f\t%.16f\t%.16f\t%.16f",
              qArr[0], qArr[1], qArr[2], qArr[3], qArr[4], qArr[5], qArr[6]));
      streamLength = pwr2LawNext(ppo, streamLength);
    }
    job.println("");
  }

  public abstract void configure();

  public abstract void prepareTrialSet(int streamLength);

  public abstract double doTrial();

  public abstract double getEpsilon();

}
