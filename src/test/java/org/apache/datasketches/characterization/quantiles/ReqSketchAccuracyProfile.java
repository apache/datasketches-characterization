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
import static org.apache.datasketches.Criteria.GE;
import static org.apache.datasketches.Criteria.LT;
import static org.apache.datasketches.ExponentiallySpacedPoints.expSpacedFloats;
import static org.apache.datasketches.GaussianRanks.GAUSSIANS_3SD;
import static org.apache.datasketches.Util.evenlySpacedFloats;
import static org.apache.datasketches.Util.pwr2LawNext;

import org.apache.datasketches.Criteria;
import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.MonotonicPoints;
import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.Shuffle;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.datasketches.req.ReqDebugImpl;
import org.apache.datasketches.req.ReqSketch;
import org.apache.datasketches.req.ReqSketchBuilder;

/**
 * @author Lee Rhodes
 */
public class ReqSketchAccuracyProfile implements JobProfile {
  private Job job;
  private Properties prop;

  //FROM PROPERTIES
  //For computing the different stream lengths
  private int lgMin;
  private int lgMax;
  private int lgDelta;
  private int ppo; //not currently used

  private int numTrials; //num of Trials per plotPoint
  private int errQSkLgK; //size of the error quantiles sketches
  private int errHllSkLgK; //size of the error HLL sketch

  //plotting & x-axis configuration
  private int numPlotPoints;
  private boolean evenlySpaced;
  private double exponent;
  private int sd;
  private double rankRange;

  //Target sketch configuration & error analysis
  private int K;
  private boolean hra; //high rank accuracy
  private boolean compatible;
  private Criteria criterion;
  private org.apache.datasketches.req.ReqDebugImpl reqDebugImpl = null;

  //DERIVED globals
  private ReqSketch sk;

  //The array of Gaussian quantiles for +/- StdDev error analysis
  private double[] gRanks;
  private UpdateDoublesSketch[] errQSkArr;
  private HllSketch[] errHllSkArr;

  //Specific to a streamLength
  private float[] stream; //a shuffled array of values from 1...N
  private float[] trueValues; //
  private int trueValueCorrection;
  private float[] corrTrueValues;

  private final String[] columnLabels =
    {"nPP", "Value", "Rank", "-3SD","-2SD", "-1SD", "Med", "+1SD", "+2SD", "+3SD", "1LB", "1UB", "U"};
  private final String sFmt =
    "%3s\t%5s\t%4s\t%4s\t%4s\t%4s\t%5s\t%4s\t%4s\t%4s\t%3s\t%3s\t%3s\n";
  private final String fFmt =
    "%14.10f\t%14.0f\t%14.10f\t" //rPP, Value, Rank
  + "%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t" //-3sd to +3sd
  + "%14.10f\t%14.10f\t%6d\n"; //1lb, 1ub, U

  //JobProfile interface
  @Override
  public void start(final Job job) {
    this.job = job;
    prop = job.getProperties();
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
    lgMin = Integer.parseInt(prop.mustGet("LgMin"));
    lgMax = Integer.parseInt(prop.mustGet("LgMax"));
    lgDelta = Integer.parseInt(prop.mustGet("LgDelta"));
    ppo = Integer.parseInt(prop.mustGet("PPO"));
    //numTrials & error quantiles & HLL sketch config
    numTrials = 1 << Integer.parseInt(prop.mustGet("LgTrials"));
    errQSkLgK = Integer.parseInt(prop.mustGet("ErrQSkLgK"));
    errHllSkLgK = Integer.parseInt(prop.mustGet("ErrHllSkLgK"));
    //plotting & x-axis config
    numPlotPoints = Integer.parseInt(prop.mustGet("NumPlotPoints"));
    evenlySpaced = Boolean.valueOf(prop.mustGet("EvenlySpaced"));
    exponent = Double.parseDouble(prop.mustGet("Exponent"));
    sd = Integer.parseInt(prop.mustGet("StdDev"));
    rankRange = Double.parseDouble(prop.mustGet("RankRange"));
    //Target sketch config
    K = Integer.parseInt(prop.mustGet("K"));
    hra = Boolean.parseBoolean(prop.mustGet("HRA"));
    compatible = Boolean.parseBoolean(prop.mustGet("Compatible"));
    criterion = Criteria.valueOf(prop.mustGet("Criterion"));
    String reqDebugLevel = prop.get("ReqDebugLevel");
    String reqDebugFmt = prop.get("ReqDebugFmt");
    if (reqDebugLevel != null) {
      int level = Integer.parseInt(reqDebugLevel);
      reqDebugImpl = new ReqDebugImpl(level, reqDebugFmt);
    }
  }

  void configureCommon() {
    configureSketch();
    trueValues = new float[numPlotPoints];
    corrTrueValues = new float[numPlotPoints];
    trueValueCorrection = criterion == GE || criterion == LT ? 1 : 0;
    errQSkArr = new UpdateDoublesSketch[numPlotPoints];
    errHllSkArr = new HllSketch[numPlotPoints];
    //configure the error quantiles array & HLL sketch arr
    final DoublesSketchBuilder builder = DoublesSketch.builder().setK(1 << errQSkLgK);
    for (int i = 0; i < numPlotPoints; i++) {
      errQSkArr[i] = builder.build();
      errHllSkArr[i] = new HllSketch(errHllSkLgK);
    }
    gRanks = new double[GAUSSIANS_3SD.length - 2]; //omit 0.0 and 1.0
    for (int i = 1; i < GAUSSIANS_3SD.length - 1; i++) {
      gRanks[i - 1] = GAUSSIANS_3SD[i];
    }
  }

  void configureSketch() {
    final ReqSketchBuilder bldr = ReqSketch.builder();
    bldr.setK(K).setHighRankAccuracy(hra).setCompatible(compatible);
    if (reqDebugImpl != null) { bldr.setReqDebug(reqDebugImpl); }
    sk = bldr.build();
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
    job.println(LS + "Stream Length: " + streamLength );
    job.printfData(sFmt, (Object[])columnLabels);

    //build the stream
    //the values themselves reflect their integer ranks starting with 1.
    stream = new float[streamLength];
    for (int sl = 1; sl <= streamLength; sl++) { stream[sl - 1] = sl; } //1 to SL

    //compute the true values used at the plot points
    final int subStreamLen = (int)Math.round(rankRange * streamLength);
    final float start = hra ? streamLength - subStreamLen : 1.0f;
    final float end = hra ? streamLength : subStreamLen;
    final float[] fltValues = evenlySpaced
        ? evenlySpacedFloats(start, end, numPlotPoints)
        : expSpacedFloats(1.0f, streamLength, numPlotPoints, exponent, hra);

    for (int pp = 0; pp < numPlotPoints; pp++) {
      trueValues[pp] = round(fltValues[pp]);
      corrTrueValues[pp] = trueValues[pp] - trueValueCorrection;
    }

    //Do numTrials for all plotpoints
    for (int t = 0; t < numTrials; t++) {
      doTrial(sk, stream, trueValues, corrTrueValues, errQSkArr, errHllSkArr);
    }

    //at this point each of the errQSkArr sketches has a distribution of error from numTrials
    for (int pp = 0 ; pp < numPlotPoints; pp++) {
      final double v = trueValues[pp];
      final double tr = v / streamLength; //the true rank
      final double rlb = sk.getRankLowerBound(tr, sd) - tr;
      final double rub = sk.getRankUpperBound(tr, sd) - tr;


      //for each of the numErrDistRanks distributions extract the sd quantiles
      final double[] errQ = errQSkArr[pp].getQuantiles(gRanks); //get error values at the Gaussian ranks
      final int errCnt = (int)round(errHllSkArr[pp].getEstimate());

      //Plot the row.
      final double relPP = (double)(pp + 1) / numPlotPoints;
      job.printfData(fFmt, relPP, v, tr,
          errQ[0], errQ[1], errQ[2], errQ[3], errQ[4], errQ[5], errQ[6],
          rlb, rub, errCnt);
      errQSkArr[pp].reset(); //reset the errQSkArr for next streamLength
      errHllSkArr[pp].reset(); //reset the errHllSkArr for next streamLength
    }
    job.println(LS + "Serialization Bytes: " + sk.getSerializationBytes());
    job.println(sk.viewCompactorDetail("%5.0f", false));
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
      final float[] corrTrueValues, final UpdateDoublesSketch[] errQSkArr, HllSketch[] errHllSkArr) {
    sk.reset();
    Shuffle.shuffle(stream);
    final int sl = stream.length;
    for (int i = 0; i < sl; i++) {
      sk.update(stream[i]);
    }
    //get estimated ranks from sketch for all plotpoints, this is a bulk operation
    final double[] estRanks = sk.getRanks(trueValues);
    final int numPP = trueValues.length;
    //compute errors and update HLL for each plotPoint
    for (int pp = 0; pp < numPP; pp++) {
      final double errorAtPlotPoint = estRanks[pp] - (double)corrTrueValues[pp] / sl;
      errQSkArr[pp].update(errorAtPlotPoint); //update each of the errQArr sketches
      errHllSkArr[pp].update(errorAtPlotPoint);
    }
  }

}
