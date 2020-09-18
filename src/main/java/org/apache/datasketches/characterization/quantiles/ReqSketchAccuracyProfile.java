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

import static java.lang.Math.round;
import static org.apache.datasketches.ExponentiallySpacedPoints.expSpacedFloats;
import static org.apache.datasketches.GaussianRanks.GAUSSIANS_2SD;
import static org.apache.datasketches.Util.evenlySpacedFloats;
import static org.apache.datasketches.Util.pwr2LawNext;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.MonotonicPoints;
import org.apache.datasketches.characterization.Shuffle;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.datasketches.req.Criteria;
import org.apache.datasketches.req.ReqSketch;

/**
 * @author Lee Rhodes
 */
public class ReqSketchAccuracyProfile implements JobProfile {
  private Job job;
  //For computing the different stream lengths
  private int lgMin;
  private int lgMax;
  private int lgDelta;
  private int ppo;

  private int numTrials; //num of Trials per plotPoint
  private int errorSkLgK; //size of the error quantiles sketches

  //plotting & x-axis configuration
  private int numPlotPoints;
  private boolean evenlySpaced;
  private double exponent;

  private final String sFmt = "%3s\t%5s\t%4s\t%4s\t%4s\t%5s\t%4s\t%4s\n";
  private final String fFmt = "%3d\t%12.0f\t%12.4f\t%12.7f\t%12.7f\t%12.7f\t%12.7f\t%12.7f\n";

  //Target sketch configuration & error analysis
  private int K;
  private boolean hra; //high rank accuracy
  private Criteria criterion;
  private ReqSketch sk;

  //The array of Gaussian quantiles for +/- StdDev error analysis
  private double[] gRanks;
  private UpdateDoublesSketch[] errQSkArr;


  //Specific to a streamLength
  private float[] stream;
  private float[] trueValues;

  //JobProfile interface
  @Override
  public void start(final Job job) {
    this.job = job;
    extractProperties();
    configureCommon();
    doJob();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

  @Override
  public void println(final Object obj) { job.println(obj); }
  //end JobProfile

  private void extractProperties() {
    lgMin = Integer.parseInt(job.getProperties().mustGet("lgMin"));
    lgMax = Integer.parseInt(job.getProperties().mustGet("lgMax"));
    lgDelta = Integer.parseInt(job.getProperties().mustGet("lgDelta"));
    ppo = Integer.parseInt(job.getProperties().mustGet("PPO"));
    numTrials = 1 << Integer.parseInt(job.getProperties().mustGet("lgTrials"));
    errorSkLgK = Integer.parseInt(job.getProperties().mustGet("errSkLgK"));
    numPlotPoints = Integer.parseInt(job.getProperties().mustGet("numPlotPoints"));
    evenlySpaced = Boolean.valueOf(job.getProperties().mustGet("evenlySpaced"));
    exponent = Double.parseDouble(job.getProperties().mustGet("exponent"));
    K = Integer.parseInt(job.getProperties().mustGet("K"));
    hra = Boolean.parseBoolean(job.getProperties().mustGet("hra"));
    criterion = Criteria.valueOf(job.getProperties().mustGet("criterion"));
  }

  void configureCommon() {
    configureSketch();
    trueValues = new float[numPlotPoints];
    errQSkArr = new UpdateDoublesSketch[numPlotPoints];
    //configure the error quantiles array
    final DoublesSketchBuilder builder = DoublesSketch.builder().setK(1 << errorSkLgK);
    for (int i = 0; i < numPlotPoints; i++) {
      errQSkArr[i] = builder.build();
    }
    gRanks = new double[GAUSSIANS_2SD.length - 2]; //omit 0.0 and 1.0
    for (int i = 1; i < GAUSSIANS_2SD.length - 1; i++) {
      gRanks[i - 1] = GAUSSIANS_2SD[i];
    }
  }

  void configureSketch() {
    sk = new ReqSketch(K, hra);
    sk.setCriterion(criterion);
  }

  private void doJob() {
    //compute the number of stream lengths for the whole job
    final int numSteps;
    final boolean useppo;
    if (lgDelta < 1) {
      numSteps = MonotonicPoints.countPoints(lgMin, lgMax, ppo);
      useppo = true;
    } else {
      numSteps = (lgMax - lgMin) / lgDelta + 1;
      useppo = false;
    }

    int streamLength = 1 << lgMin; //initial streamLength
    int lgCurSL = lgMin;

    // Step through the different stream lengths
    for (int step = 0; step < numSteps; step++) {
      doStreamLength(streamLength);
      //go to next stream length
      if (useppo) {
        streamLength = pwr2LawNext(ppo, streamLength);
      } else {
        lgCurSL += lgDelta;
        streamLength = 1 << lgCurSL;
      }
    }
  }

  void doStreamLength(final int streamLength) {
    //print at the top of each stream length
    job.println(LS + "Stream Length: " + streamLength );
    job.printf(sFmt, "PP", "Value", "Rank", "-2SD", "-1SD", "Med", "+1SD", "+2SD");

    //build the stream
    //the values themselves reflect their integer ranks starting with 1.
    stream = new float[streamLength];
    for (int sl = 1; sl <= streamLength; sl++) { stream[sl - 1] = sl; } //1 to SL

    //compute the true values used at the plot points, the number of true values is constant
    final float[] fltValues = evenlySpaced
        ? evenlySpacedFloats(1.0f, streamLength, numPlotPoints)
        : expSpacedFloats(1.0f, streamLength, numPlotPoints, exponent, hra);
    for (int pp = 0; pp < numPlotPoints; pp++) {
      trueValues[pp] = round(fltValues[pp]);
    }

    //Do numTrials for all plotpoints
    doTrials(sk, stream, trueValues, errQSkArr, numTrials);

    //at this point each of the errQSkArr sketches has a distribution of error from numTrials
    for (int pp = 0 ; pp < numPlotPoints; pp++) {
      final double v = trueValues[pp]; //the true values
      final double r = v / streamLength; //the true rank

      //for each of the numErrDistRanks distributions extract the sd quantiles
      final double[] errQ = errQSkArr[pp].getQuantiles(gRanks); //get error values at the Gaussian ranks

      //Plot the row. We ignore quantiles collected at 0 and 1.0.
      job.printf(fFmt, pp + 1, v, r, errQ[0], errQ[1], errQ[2], errQ[3], errQ[4]);
      errQSkArr[pp].reset(); //reset the errQSkArr for next streamLength
    }
  }

  /**
   * A set of trials for all plot points.
   * @param stream the set of value to feed the sketch
   * @param trueValues the true integer values at the plot points
   * @param errQSkArr the quantile error sketches for each plot point
   */
  static void doTrials(final ReqSketch sk, final float[] stream, final float[] trueValues,
      final UpdateDoublesSketch[] errQSkArr, final int numTrials) {
    for (int t = 0; t < numTrials; t++) {
      doTrial(sk, stream, trueValues, errQSkArr);
    }
  }

  /**
   * A trial consists of updating a virgin sketch with a shuffled stream of streamLength values.
   * We capture the estimated ranks for all plotPoints and then update the errQSkArr with those
   * error values.
   * @param stream the source stream
   * @param trueValues the true integer ranks at each of the plot points
   * @param errQSkArr the quantile error sketches for each plot point to be updated
   */
  static void doTrial(final ReqSketch sk, final float[] stream, final float[] trueValues,
      final UpdateDoublesSketch[] errQSkArr) {
    sk.reset();
    Shuffle.shuffle(stream);
    final int sl = stream.length;
    for (int i = 0; i < sl; i++) {
      sk.update(stream[i]);
    }
    //get estimated ranks from sketch for all plotpoints, this is a bulk operation
    final double[] estRanks = sk.getRanks(trueValues);
    final int numPP = trueValues.length;
    //compute errors for each plotPoint
    for (int pp = 0; pp < numPP; pp++) {
      final double errorAtPlotPoint = estRanks[pp] - (double)trueValues[pp] / sl;
      errQSkArr[pp].update(errorAtPlotPoint); //update each of the errQArr sketches
    }
  }

}
