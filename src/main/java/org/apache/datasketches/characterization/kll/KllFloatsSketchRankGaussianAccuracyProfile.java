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

import static java.lang.Math.round;
import static org.apache.datasketches.GaussianRanks.GAUSSIANS_3SD;
import static org.apache.datasketches.common.Util.pwr2SeriesNext;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantilesUtil.evenlySpacedFloats;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.MonotonicPoints;
import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.Shuffle;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.kll.KllSketch;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

/**
 * @author Lee Rhodes
 */
public class KllFloatsSketchRankGaussianAccuracyProfile implements JobProfile {
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();
  private Job job;
  private Properties props;

  //FROM PROPERTIES
  //For computing the different stream lengths
  private int lgMin;
  private int lgMax;
  private int lgDelta;
  private int ppo; //not used for rank accuracy

  private int numTrials; //num of Trials per plotPoint
  private int errorSkLgK; //size of the error quantiles sketches

  //plotting & x-axis configuration
  private int numPlotPoints;

  //Target sketch configuration & error analysis
  private int k;
  private QuantileSearchCriteria criteria;
  private boolean useBulk;
  private boolean direct;

  //DERIVED globals
  private KllFloatsSketch sk;

  //The array of Gaussian quantiles for +/- StdDev error analysis
  private double[] gRanks;
  private UpdateDoublesSketch[] errQSkArr;

  //Specific to a streamLength
  private float[] stream;
  private float[] trueNaturalRanks;
  private int trueValueCorrection;
  private float[] corrNaturalRanks;

  private final String[] columnLabels =
    {"nPP", "Value", "Rank", "-3SD","-2SD", "-1SD", "Med", "+1SD", "+2SD", "+3SD"};
  private final String sFmt =
    "%3s\t%5s\t%4s\t%4s\t%4s\t%4s\t%5s\t%4s\t%4s\t%4s\n";
  private final String fFmt =
    "%14.10f\t%14.0f\t%14.10f\t" //rPP, Value, Rank
  + "%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t\n"; //-3sd to +3sd

  //JobProfile interface
  @Override
  public void start(final Job job) {
    this.job = job;
    this.props = job.getProperties();
    extractProperties();
    configureCommon();
    doJob();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}
  //end JobProfile

  private void extractProperties() {
    //stream length
    lgMin = Integer.parseInt(props.mustGet("LgMin"));
    lgMax = Integer.parseInt(props.mustGet("LgMax"));
    lgDelta = Integer.parseInt(props.mustGet("LgDelta"));
    ppo = Integer.parseInt(props.mustGet("PPO"));
    //numTrials & error quantiles sketch config
    numTrials = 1 << Integer.parseInt(props.mustGet("LgTrials"));
    errorSkLgK = Integer.parseInt(props.mustGet("ErrSkLgK"));
    //plotting & x-axis config
    numPlotPoints = Integer.parseInt(props.mustGet("NumPlotPoints"));
    //Target sketch config
    k = Integer.parseInt(props.mustGet("K"));
    criteria = props.mustGet("criteria").equalsIgnoreCase("INCLUSIVE") ? INCLUSIVE : EXCLUSIVE;
    useBulk = Boolean.parseBoolean(props.mustGet("useBulk"));
    direct = Boolean.parseBoolean(props.mustGet("direct"));
  }

  void configureCommon() {
    configureSketch();
    trueNaturalRanks = new float[numPlotPoints];
    corrNaturalRanks = new float[numPlotPoints];
    trueValueCorrection = (criteria == EXCLUSIVE) ? 1 : 0;
    errQSkArr = new UpdateDoublesSketch[numPlotPoints];
    //configure the error quantiles array
    final DoublesSketchBuilder builder = DoublesSketch.builder().setK(1 << errorSkLgK);
    for (int i = 0; i < numPlotPoints; i++) {
      errQSkArr[i] = builder.build();
    }
    gRanks = new double[GAUSSIANS_3SD.length - 2]; //omit 0.0 and 1.0
    for (int i = 1; i < GAUSSIANS_3SD.length - 1; i++) {
      gRanks[i - 1] = GAUSSIANS_3SD[i];
    }
  }

  void configureSketch() {
    if (direct) {
      final WritableMemory wmem = WritableMemory.allocate(10000);
      sk = KllFloatsSketch.newDirectInstance(k, wmem, memReqSvr);
    } else {
      sk = KllFloatsSketch.newHeapInstance(k);
    }
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
        streamLength = (int)pwr2SeriesNext(ppo, streamLength);
      } else {
        lgCurSL += lgDelta;
        streamLength = 1 << lgCurSL;
      }
    }
  }

  void doStreamLength(final int streamLength) {
    job.println(LS + "Stream Length: " + streamLength );
    job.println("Epsilon: " + KllSketch.getNormalizedRankError(k, false));
    job.printfData(sFmt, (Object[])columnLabels);

    //build the stream
    stream = new float[streamLength];
    for (int sl = 1; sl <= streamLength; sl++) { stream[sl - 1] = sl; } //1 to SL

    //compute the true values used at the plot points
    final float start = 1.0f;
    final float end = streamLength;
    //approxFltNatRanks are designed to be their natural ranks starting with 1
    final float[] approxFltNatRanks = evenlySpacedFloats(start, end, numPlotPoints);

    for (int pp = 0; pp < numPlotPoints; pp++) {
      trueNaturalRanks[pp] = round(approxFltNatRanks[pp]); //force nearest integer
      corrNaturalRanks[pp] = trueNaturalRanks[pp] - trueValueCorrection; //minus: exclusive = 1, inclusive = 0
    }

    //Do numTrials for all plot points
    for (int t = 0; t < numTrials; t++) {
      sk.reset();
      doTrial(sk, stream, trueNaturalRanks, corrNaturalRanks, errQSkArr);
    }

    //at this point each of the errQSkArr sketches has a distribution of error from numTrials
    for (int pp = 0 ; pp < numPlotPoints; pp++) {
      final double trueNatRank = trueNaturalRanks[pp];
      final double trueNormalizedRank = trueNatRank / streamLength; //create the true normalized rank

      //for each of the numErrDistRanks distributions extract the sd quantiles
      final double[] errQ = errQSkArr[pp].getQuantiles(gRanks); //get error values at the Gaussian ranks

      //Plot the row. We ignore quantiles collected at 0 and 1.0.
      final double relPP = (double)(pp + 1) / numPlotPoints;
      job.printfData(fFmt, relPP, trueNatRank, trueNormalizedRank,
          errQ[0], errQ[1], errQ[2], errQ[3], errQ[4], errQ[5], errQ[6]);
      errQSkArr[pp].reset(); //reset the errQSkArr for next streamLength
    }
    job.println(LS + "Serialization Bytes: " + sk.getSerializedSizeBytes());
  }

  /**
   * A trial consists of updating a virgin sketch with a shuffled stream of streamLength values.
   * We capture the estimated ranks for all plotPoints and then update the errQSkArr with those
   * error values.
   * @param sk the sketch under test
   * @param stream the source stream
   * @param trueNatRanks input quantiles to getRanks and also true natural ranks at each of the plot points
   * @param corrNatRanks corrected true values based on the comparison inequality
   * @param errQSkArr the quantile error sketches for each plot point to be updated
   */
  void doTrial(final KllFloatsSketch sk, final float[] stream, final float[] trueNatRanks,
      final float[] corrNatRanks, final UpdateDoublesSketch[] errQSkArr) {
    Shuffle.shuffle(stream);
    final int sl = stream.length;
    for (int i = 0; i < sl; i++) {
      sk.update(stream[i]);
    }
    final int numPP = trueNatRanks.length;
    //get estimated ranks from sketch for all plot points
    final double[] estRanks;
    if (useBulk) {
      estRanks = sk.getRanks(trueNatRanks, criteria);
    } else {
      estRanks = new double[numPP];
      for (int pp = 0; pp < numPP; pp++) {
        estRanks[pp] = sk.getRank(trueNatRanks[pp], criteria);
      }
    }

    //compute errors for each plotPoint
    for (int pp = 0; pp < numPP; pp++) {
      final double errorAtPlotPoint = estRanks[pp] - (double)corrNatRanks[pp] / sl;
      errQSkArr[pp].update(errorAtPlotPoint); //update each of the errQArr sketches
    }
  }

}
