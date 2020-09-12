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

package org.apache.datasketches.characterization.quantiles;

import static org.apache.datasketches.GaussianRanks.GAUSSIANS_2SD;
import static org.apache.datasketches.Util.evenlySpacedFloats;
import static org.apache.datasketches.Util.pwr2LawNext;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.MonotonicPoints;
import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.Shuffle;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

/**
 * @author Lee Rhodes
 */
public abstract class BaseReqSketchAccuracyProfile implements JobProfile {

  Job job;
  private DoublesSketchBuilder builder; //for error analysis
  float[] stream;
  double[] evenlySpacedRanks;
  float[] evenlySpacedValues;
  int numEvenlySpaced;

  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
    doTrials();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

  @Override
  public void println(final Object obj) {
    job.println(obj);
  }
  //end JobProfile

  private void doTrials() {
    final int lgMin = Integer.parseInt(job.getProperties().mustGet("lgMin"));
    final int lgMax = Integer.parseInt(job.getProperties().mustGet("lgMax"));
    final int lgDelta = Integer.parseInt(job.getProperties().mustGet("lgDelta"));
    final int ppo = Integer.parseInt(job.getProperties().mustGet("PPO"));
    final int lgTrials = Integer.parseInt(job.getProperties().mustGet("lgTrials"));
    final int errorSketchLgK = Integer.parseInt(job.getProperties().mustGet("errLgK"));
    numEvenlySpaced = Integer.parseInt(job.getProperties().mustGet("numEvenlySpaced"));

    builder = DoublesSketch.builder().setK(1 << errorSketchLgK);

    configure(job.getProperties());

    final int numSteps;
    final boolean useppo;
    if (lgDelta < 1) {
      numSteps = MonotonicPoints.countPoints(lgMin, lgMax, ppo);
      useppo = true;
    } else {
      numSteps = (lgMax - lgMin) / lgDelta + 1;
      useppo = false;
    }

    final UpdateDoublesSketch[] errQArr = new UpdateDoublesSketch[numEvenlySpaced];
    for (int i = 1; i < numEvenlySpaced; i++) {
      errQArr[i] = builder.build();
    }

    final double[] gRanks = GAUSSIANS_2SD;
    //final int gRlen = gRanks.length;

    //top of report for this streamlength
    final String sFmt = "%12s\t%12s\t%12s\t%12s\t%12s\t%12s\t%12s\t%12s\n";
    final String fFmt = "%12.0f\t%12.4f\t%12.7f\t%12.7f\t%12.7f\t%12.7f\t%12.7f\t%12.7f\n";

    int streamLength = 1 << lgMin;
    int lgCur = lgMin;
    for (int i = 0; i < numSteps; i++) {
      job.println(LS + "Stream Length: " + streamLength + ", lgStreamLength: " + lgCur);
      job.printf(sFmt, "Value", "Rank", "-2SD", "-1SD", "Med", "+1SD", "+2SD", "1.0");

      //build the stream
      stream = new float[streamLength];
      for (int j = 1; j <= streamLength; j++) { stream[j - 1] = j; }
      //construct the evenly spaced values
      evenlySpacedValues = evenlySpacedFloats(0.0f, streamLength, numEvenlySpaced);

      //to eliminate possible numerical problems we compute the true ranks individually.
      evenlySpacedRanks = new double[numEvenlySpaced];
      for (int j = 0; j < numEvenlySpaced; j++) {
        evenlySpacedValues[j] = Math.round(evenlySpacedValues[j]);
        evenlySpacedRanks[j] = (double)evenlySpacedValues[j] / streamLength;
      }

      final int numTrials = 1 << lgTrials;
      for (int t = 0; t < numTrials; t++) {
        Shuffle.shuffle(stream);

        //In Trial: update reqSk with streamLength values
        //returns numErrDistRanks errors from numErrDistRanks ranks
        final double[] errorsFromTrial = doTrial();
        for (int j = 1; j < numEvenlySpaced; j++) { //get err from the evenly spaced points
          errQArr[j].update(errorsFromTrial[j]); //update each of the numErrDistRanks errQArr sketches
        }
      }
      //at this point each of the evenly spaced sketches has a distribution of error from numTrials

      for (int j = 1 ; j < numEvenlySpaced; j++) {
        final double r = evenlySpacedRanks[j]; //the true ranks
        final float v = evenlySpacedValues[j]; //the true values
        //for each of the numErrDistRanks distributions extract the 7 sd quantiles
        // and subtract from the true rank
        final double[] errQ = errQArr[j].getQuantiles(gRanks); //get re values at the Gaussian ranks
        job.printf(fFmt, v, r, errQ[1], errQ[2], errQ[3], errQ[4], errQ[5], errQ[6]);
      }

      //go to next stream length
      if (useppo) {
        streamLength = pwr2LawNext(ppo, streamLength);
      } else {
        lgCur += lgDelta;
        streamLength = 1 << lgCur;
      }
    }
  }

  abstract void configure(Properties props);

  abstract void prepareTrial();

  abstract double[] doTrial();

}
