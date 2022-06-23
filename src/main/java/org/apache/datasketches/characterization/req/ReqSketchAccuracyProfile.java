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

import static java.lang.Math.round;
import static org.apache.datasketches.GaussianRanks.GAUSSIANS_3SD;
import static org.apache.datasketches.SpacedPoints.expSpaced;
import static org.apache.datasketches.Util.evenlySpaced;
import static org.apache.datasketches.Util.pwr2SeriesNext;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.MonotonicPoints;
import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.Shuffle;
import org.apache.datasketches.characterization.req.StreamMaker.Pattern;
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
  //Stream pattern config
  StreamMaker streamMaker = new StreamMaker();
  private Pattern pattern;
  private int offset;

  //For computing the different stream lengths
  private int lgMin;
  private int lgMax;
  private int lgDelta;
  private int ppo; //not currently used

  private int numTrials; //num of Trials per plotPoint
  private int errQSkLgK; //size of the error quantiles sketches
  private int errHllSkLgK; //size of the error HLL sketch
  private boolean shuffle; //if true, shuffle for each trial

  //plotting & x-axis configuration
  private int numPlotPoints;
  private boolean evenlySpaced;
  private double exponent;
  private int sd;
  private double rankRange;
  private double metricsRankRange;

  //Target sketch configuration & error analysis
  private int K;
  private boolean hra; //high rank accuracy
  private boolean ltEq;
  private org.apache.datasketches.req.ReqDebugImpl reqDebugImpl = null;


  // TEMPORARY
  int INIT_NUMBER_OF_SECTIONS;
  float NOM_CAPACITY_MULTIPLIER;
  int MIN_K;
  boolean LAZY_COMPRESSION;

  //DERIVED globals
  private ReqSketch sk;

  //The array of Gaussian quantiles for +/- StdDev error analysis
  private double[] gRanks;
  private UpdateDoublesSketch[] errQSkArr;
  private HllSketch[] errHllSkArr;

  //Specific to a streamLength
  private TrueFloatRanks trueRanks;
  //The entire stream
  private float[] stream; //a shuffled array of values from 1...N
  private float[] sortedStream;
  private int[] sortedAbsRanks;
  //private int[] streamAbsRanks ?? do we need?
  //The PP points
  private float[] sortedPPValues;
  private int[] sortedPPIndices;
  private int[] sortedPPAbsRanks;
  int sumAllocCounts = 0;

  private final String[] columnLabels =
    {"nPP", "Value", "Rank",
     "-3SD","-2SD", "-1SD", "Med", "+1SD", "+2SD", "+3SD",
     "1LB", "1UB", "UErrCnt"};
  private final String sFmt =
      "%3s\t%5s\t%4s\t"
    + "%4s\t%4s\t%4s\t%5s\t%4s\t%4s\t%4s\t"
    + "%3s\t%3s\t%7s\n";
  private final String fFmt =
    "%14.10f\t%14.0f\t%14.10f\t" //rPP, Value, Rank
  + "%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t" //-3sd to +3sd
  + "%14.10f\t%14.10f\t%6d\n"; //1lb, 1ub, UErrCnt

  //JobProfile interface
  @Override
  public void start(final Job job) {
    this.job = job;
    prop = job.getProperties();
    extractProperties();
    configureCommon();
    doStreamLengths();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}
  //end JobProfile

  private void extractProperties() {
    //Stream Pattern
    pattern = Pattern.valueOf(prop.mustGet("Pattern"));
    offset = Integer.parseInt(prop.mustGet("Offset"));
    //Stream lengths
    lgMin = Integer.parseInt(prop.mustGet("LgMin"));
    lgMax = Integer.parseInt(prop.mustGet("LgMax"));
    lgDelta = Integer.parseInt(prop.mustGet("LgDelta"));
    ppo = Integer.parseInt(prop.mustGet("PPO"));
    // Trials config (independent of sketch)
    numTrials = 1 << Integer.parseInt(prop.mustGet("LgTrials"));
    errQSkLgK = Integer.parseInt(prop.mustGet("ErrQSkLgK"));
    errHllSkLgK = Integer.parseInt(prop.mustGet("ErrHllSkLgK"));
    shuffle = Boolean.valueOf(prop.mustGet("Shuffle"));
    //plotting
    numPlotPoints = Integer.parseInt(prop.mustGet("NumPlotPoints"));
    evenlySpaced = Boolean.valueOf(prop.mustGet("EvenlySpaced"));
    exponent = Double.parseDouble(prop.mustGet("Exponent"));
    sd = Integer.parseInt(prop.mustGet("StdDev"));
    rankRange = Double.parseDouble(prop.mustGet("RankRange"));
    //Target sketch config
    K = Integer.parseInt(prop.mustGet("K"));
    hra = Boolean.parseBoolean(prop.mustGet("HRA"));
    ltEq = Boolean.parseBoolean(prop.mustGet("LtEq"));


    metricsRankRange = Double.parseDouble(prop.mustGet("MetricsRankRange"));

    INIT_NUMBER_OF_SECTIONS = Integer.parseInt(prop.mustGet("INIT_NUMBER_OF_SECTIONS"));
    NOM_CAPACITY_MULTIPLIER = Float.parseFloat(prop.mustGet("NOM_CAPACITY_MULTIPLIER"));
    MIN_K = Integer.parseInt(prop.mustGet("MIN_K"));
    LAZY_COMPRESSION = Boolean.parseBoolean(prop.mustGet("LAZY_COMPRESSION"));
    //criterion = InequalitySearch.valueOf(prop.mustGet("Criterion"));
    final String reqDebugLevel = prop.get("ReqDebugLevel");
    final String reqDebugFmt = prop.get("ReqDebugFmt");
    if (reqDebugLevel != null) {
      final int level = Integer.parseInt(reqDebugLevel);
      reqDebugImpl = new ReqDebugImpl(level, reqDebugFmt);
    }
  }

  void configureCommon() {
    configureSketch();
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
    bldr.setK(K).setHighRankAccuracy(hra);
    if (reqDebugImpl != null) { bldr.setReqDebug(reqDebugImpl); }
    sk = bldr.build();
    sk.setLessThanOrEqual(ltEq);
  }

  private void doStreamLengths() {
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
        streamLength = pwr2SeriesNext(ppo, streamLength);
      } else {
        lgCurSL += lgDelta;
        streamLength = 1 << lgCurSL;
      }
    }
  }

  void doStreamLength(final int streamLength) {
    job.println(LS + "Stream Length: " + streamLength );
    job.println(LS + "param k: " + K );
    job.printfData(sFmt, (Object[])columnLabels);
    //build the stream
    stream = streamMaker.makeStream(streamLength, pattern, offset);
    //compute true ranks
    if (ltEq) {
      trueRanks = new TrueFloatRanks(stream, true);
    } else {
      trueRanks = new TrueFloatRanks(stream, false);
    }
    sortedStream = trueRanks.getSortedStream();
    sortedAbsRanks = trueRanks.getSortedAbsRanks();

    //compute the true values used at the plot points
    int startIdx = 0;
    int endIdx = streamLength - 1;
    if (rankRange < 1.0) { //A substream of points focuses on a sub-range at one end.
      final int subStreamLen = (int)Math.round(rankRange * streamLength);
      startIdx = hra ? streamLength - subStreamLen : 0;
      endIdx = hra ? streamLength - 1 : subStreamLen - 1;
    }

    //generates PP indices in [startIdx, endIdx] inclusive, inclusive
    // PV 2020-01-07: using double so that there's enough precision even for large stream lengths
    final double[] temp = evenlySpaced
        ? evenlySpaced(startIdx, endIdx, numPlotPoints)
        : expSpaced(startIdx, endIdx, numPlotPoints, exponent, hra);

    sortedPPIndices = new int[numPlotPoints];
    sortedPPAbsRanks = new int[numPlotPoints];
    sortedPPValues = new float[numPlotPoints];

    for (int pp = 0; pp < numPlotPoints; pp++) {
      final int idx = (int)Math.round(temp[pp]);
      sortedPPIndices[pp] = idx;
      sortedPPAbsRanks[pp] = sortedAbsRanks[idx];
      sortedPPValues[pp] = sortedStream[idx];
    }

    //Do numTrials for all plotpoints
    for (int t = 0; t < numTrials; t++) {
      doTrial();

      //sumAllocCounts = sk.
    }

    // for special metrics for capturing accuracy per byte
    double sumRelStdDev = 0;
    int numRelStdDev = 0;
    double sumAddStdDev = 0;
    int numAddStdDev = 0;

    //at this point each of the errQSkArr sketches has a distribution of error from numTrials
    for (int pp = 0 ; pp < numPlotPoints; pp++) {
      final double v = sortedPPValues[pp];
      final double tr = v / streamLength; //the true rank
      final double rlb = sk.getRankLowerBound(tr, sd) - tr;
      final double rub = sk.getRankUpperBound(tr, sd) - tr;

      //for each of the numErrDistRanks distributions extract the sd Gaussian quantiles
      final double[] errQ = errQSkArr[pp].getQuantiles(gRanks);
      final int uErrCnt = (int)round(errHllSkArr[pp].getEstimate());

      //Plot the row.
      final double relPP = (double)(pp + 1) / numPlotPoints;
      job.printfData(fFmt, relPP, v, tr,
          errQ[0], errQ[1], errQ[2], errQ[3], errQ[4], errQ[5], errQ[6],
          rlb, rub, uErrCnt);

      if (relPP > 0 && relPP < 1
          && (hra && relPP < metricsRankRange || !hra && relPP >= 1 - metricsRankRange)) {
        sumAddStdDev += errQ[4];
        numAddStdDev++;
      }
      if (relPP > 0 && relPP < 1
          && (!hra && relPP < metricsRankRange || hra && relPP >= 1 - metricsRankRange)) {
        sumRelStdDev += errQ[4] / (hra ? 1 - relPP : relPP);
        numRelStdDev++;
      }
      errQSkArr[pp].reset(); //reset the errQSkArr for next streamLength
      errHllSkArr[pp].reset(); //reset the errHllSkArr for next streamLength
    }
    final int serBytes = sk.getSerializationBytes();

    // special metrics for capturing accuracy per byte
    final double avgRelStdDevTimesSize = serBytes * sumRelStdDev / numRelStdDev;
    final  double avgAddStdDevTimesSize = serBytes * sumAddStdDev / numAddStdDev;
    job.println(LS + "Avg. relative std. dev. times size: " + avgRelStdDevTimesSize);
    job.println(     "Avg. additive std. dev. times size: " + avgAddStdDevTimesSize);

    job.println(LS + "Serialization Bytes: " + serBytes);
    job.println(sk.viewCompactorDetail("%5.0f", false));
  }

  /**
   * A trial consists of updating a virgin sketch with a stream of values.
   * Capture the estimated ranks for all plotPoints and then update the errQSkArr with those
   * error values.
   */
  void doTrial() {
    sk.reset();
    if (shuffle) { Shuffle.shuffle(stream); }
    final int sl = stream.length;
    for (int i = 0; i < sl; i++) { sk.update(stream[i]); }
    //get estimated ranks from sketch for all plotpoints
    final double[] estRanks = sk.getRanks(sortedPPValues);
    //compute errors and update HLL for each plotPoint
    for (int pp = 0; pp < numPlotPoints; pp++) {
      final double errorAtPlotPoint = estRanks[pp] - (double)sortedPPAbsRanks[pp] / sl;
      errQSkArr[pp].update(errorAtPlotPoint); //update each of the errQArr sketches
      errHllSkArr[pp].update(errorAtPlotPoint); //unique count of error values
    }
  }

}
