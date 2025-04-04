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

//package org.apache.datasketches.characterization.tdigest;
//
//import static java.lang.Math.round;
//import static org.apache.datasketches.GaussianRanks.GAUSSIANS_3SD;
//import static org.apache.datasketches.SpacedPoints.expSpaced;
//import static org.apache.datasketches.common.Util.pwr2SeriesNext;
//import static org.apache.datasketches.quantilescommon.QuantilesUtil.evenlySpacedDoubles;
//
//import org.apache.datasketches.Job;
//import org.apache.datasketches.JobProfile;
//import org.apache.datasketches.MonotonicPoints;
//import org.apache.datasketches.Properties;
//import org.apache.datasketches.characterization.Shuffle;
//import org.apache.datasketches.characterization.req.StreamMaker;
//import org.apache.datasketches.characterization.req.StreamMaker.Pattern;
//import org.apache.datasketches.hll.HllSketch;
//import org.apache.datasketches.quantiles.DoublesSketch;
//import org.apache.datasketches.quantiles.DoublesSketchBuilder;
//import org.apache.datasketches.quantiles.UpdateDoublesSketch;
//
//import com.tdunning.math.stats.TDigest;
//
///**
// * @author Lee Rhodes
// */
//public class TDigestErrorVsRankProfile implements JobProfile {
//  private Job job;
//  private Properties prop;
//
//  //FROM PROPERTIES
//  //Stream pattern config
//  StreamMaker streamMaker = new StreamMaker();
//  private Pattern pattern;
//  private int offset;
//
//  //For computing the different stream lengths
//  private int lgMin;
//  private int lgMax;
//  private int lgDelta;
//  private int ppo; //not currently used
//
//  private int numTrials; //num of Trials per plotPoint
//  private int errQSkLgK; //size of the error quantiles sketches
//  private int errHllSkLgK; //size of the error HLL sketch
//  private boolean shuffle; //if true, shuffle for each trial
//
//  //plotting & x-axis configuration
//  private int numPlotPoints;
//  private boolean evenlySpaced;
//  private double exponent;
//  private double rankRange;
//  private double metricsRankRange;
//
//  //Target sketch configuration & error analysis
//  private int K;
//  private TDigest sk;
//
//  //The array of Gaussian quantiles for +/- StdDev error analysis
//  private double[] gRanks;
//  private UpdateDoublesSketch[] errQSkArr;
//  private HllSketch[] errHllSkArr;
//
//  //Specific to a streamLength
//  private TrueDoubleRanks trueRanks;
//  //The entire stream
//  private float[] stream; //a shuffled array of values from 1...N
//  private float[] sortedStream;
//  private int[] sortedAbsRanks;
//  //private int[] streamAbsRanks ?? do we need?
//  //The PP points
//  private float[] sortedPPValues;
//  private int[] sortedPPIndices;
//  private int[] sortedPPAbsRanks;
//  int sumAllocCounts = 0;
//
//  private final String[] columnLabels =
//    {"nPP", "Value", "Rank",
//     "-3SD","-2SD", "-1SD", "Med", "+1SD", "+2SD", "+3SD",
//     "1LB", "1UB", "UErrCnt"};
//  private final String sFmt =
//      "%3s\t%5s\t%4s\t"
//    + "%4s\t%4s\t%4s\t%5s\t%4s\t%4s\t%4s\t"
//    + "%3s\t%3s\t%7s\n";
//  private final String fFmt =
//    "%f\t%f\t%f\t" // rPP, Value, Rank
//  + "%f\t%f\t%f\t%f\t%f\t%f\t%f\t" //-3sd to +3sd
//  + "\t%d\n"; // UErrCnt
//
//  //JobProfile interface
//  @Override
//  public void start(final Job job) {
//    this.job = job;
//    prop = job.getProperties();
//    extractProperties();
//    configureCommon();
//    doStreamLengths();
//  }
//
//  @Override
//  public void shutdown() {}
//
//  @Override
//  public void cleanup() {}
//  //end JobProfile
//
//  private void extractProperties() {
//    //Stream Pattern
//    pattern = Pattern.valueOf(prop.mustGet("Pattern"));
//    offset = Integer.parseInt(prop.mustGet("Offset"));
//    //Stream lengths
//    lgMin = Integer.parseInt(prop.mustGet("LgMin"));
//    lgMax = Integer.parseInt(prop.mustGet("LgMax"));
//    lgDelta = Integer.parseInt(prop.mustGet("LgDelta"));
//    ppo = Integer.parseInt(prop.mustGet("PPO"));
//    // Trials config (independent of sketch)
//    numTrials = 1 << Integer.parseInt(prop.mustGet("LgTrials"));
//    errQSkLgK = Integer.parseInt(prop.mustGet("ErrQSkLgK"));
//    errHllSkLgK = Integer.parseInt(prop.mustGet("ErrHllSkLgK"));
//    shuffle = Boolean.valueOf(prop.mustGet("Shuffle"));
//    //plotting
//    numPlotPoints = Integer.parseInt(prop.mustGet("NumPlotPoints"));
//    evenlySpaced = Boolean.valueOf(prop.mustGet("EvenlySpaced"));
//    exponent = Double.parseDouble(prop.mustGet("Exponent"));
//    rankRange = Double.parseDouble(prop.mustGet("RankRange"));
//    //Target sketch config
//    K = Integer.parseInt(prop.mustGet("K"));
//
//    metricsRankRange = Double.parseDouble(prop.mustGet("MetricsRankRange"));
//  }
//
//  void configureCommon() {
//    configureSketch();
//    errQSkArr = new UpdateDoublesSketch[numPlotPoints];
//    errHllSkArr = new HllSketch[numPlotPoints];
//    //configure the error quantiles array & HLL sketch arr
//    final DoublesSketchBuilder builder = DoublesSketch.builder().setK(1 << errQSkLgK);
//    for (int i = 0; i < numPlotPoints; i++) {
//      errQSkArr[i] = builder.build();
//      errHllSkArr[i] = new HllSketch(errHllSkLgK);
//    }
//    gRanks = new double[GAUSSIANS_3SD.length - 2]; //omit 0.0 and 1.0
//    for (int i = 1; i < GAUSSIANS_3SD.length - 1; i++) {
//      gRanks[i - 1] = GAUSSIANS_3SD[i];
//    }
//  }
//
//  void configureSketch() {
//  }
//
//  private void doStreamLengths() {
//    //compute the number of stream lengths for the whole job
//    final int numSteps;
//    final boolean useppo;
//    if (lgDelta < 1) {
//      numSteps = MonotonicPoints.countPoints(lgMin, lgMax, ppo);
//      useppo = true;
//    } else {
//      numSteps = (lgMax - lgMin) / lgDelta + 1;
//      useppo = false;
//    }
//
//    int streamLength = 1 << lgMin; //initial streamLength
//    int lgCurSL = lgMin;
//
//    // Step through the different stream lengths
//    for (int step = 0; step < numSteps; step++) {
//
//      doStreamLength(streamLength);
//
//      //go to next stream length
//      if (useppo) {
//        streamLength = (int)pwr2SeriesNext(ppo, streamLength);
//      } else {
//        lgCurSL += lgDelta;
//        streamLength = 1 << lgCurSL;
//      }
//    }
//  }
//
//  void doStreamLength(final int streamLength) {
//    job.println(LS + "Stream Length: " + streamLength );
//    job.println(LS + "param k: " + K );
//    job.printfData(sFmt, (Object[])columnLabels);
//    //build the stream
//    stream = streamMaker.makeStream(streamLength, pattern, offset);
//    //compute true ranks
//    trueRanks = new TrueDoubleRanks(stream);
//    sortedStream = trueRanks.getSortedStream();
//    sortedAbsRanks = trueRanks.getSortedAbsRanks();
//
//    //compute the true values used at the plot points
//    int startIdx = 0;
//    int endIdx = streamLength - 1;
//    final boolean hra = true;
//    if (rankRange < 1.0) { //A substream of points focuses on a sub-range at one end.
//      final int subStreamLen = (int)Math.round(rankRange * streamLength);
//      startIdx = hra ? streamLength - subStreamLen : 0;
//      endIdx = hra ? streamLength - 1 : subStreamLen - 1;
//    }
//
//    //generates PP indices in [startIdx, endIdx] inclusive, inclusive
//    // PV 2020-01-07: using double so that there's enough precision even for large stream lengths
//    final double[] temp = evenlySpaced
//        ? evenlySpacedDoubles(startIdx, endIdx, numPlotPoints)
//        : expSpaced(startIdx, endIdx, numPlotPoints, exponent, hra);
//
//    sortedPPIndices = new int[numPlotPoints];
//    sortedPPAbsRanks = new int[numPlotPoints];
//    sortedPPValues = new float[numPlotPoints];
//
//    for (int pp = 0; pp < numPlotPoints; pp++) {
//      final int idx = (int)Math.round(temp[pp]);
//      sortedPPIndices[pp] = idx;
//      sortedPPAbsRanks[pp] = sortedAbsRanks[idx];
//      sortedPPValues[pp] = sortedStream[idx];
//    }
//
//    //Do numTrials for all plotpoints
//    for (int t = 0; t < numTrials; t++) {
//      doTrial();
//
//      //sumAllocCounts = sk.
//    }
//
//    // for special metrics for capturing accuracy per byte
//    double sumRelStdDev = 0;
//    int numRelStdDev = 0;
//    double sumAddStdDev = 0;
//    int numAddStdDev = 0;
//
//    //at this point each of the errQSkArr sketches has a distribution of error from numTrials
//    for (int pp = 0 ; pp < numPlotPoints; pp++) {
//      final double v = sortedPPValues[pp];
//      final double tr = v / streamLength; //the true rank
//
//      //for each of the numErrDistRanks distributions extract the sd Gaussian quantiles
//      final double[] errQ = errQSkArr[pp].getQuantiles(gRanks);
//      final int uErrCnt = (int)round(errHllSkArr[pp].getEstimate());
//
//      //Plot the row.
//      final double relPP = (double)(pp + 1) / numPlotPoints;
//      job.printfData(fFmt, relPP, v, tr,
//          errQ[0], errQ[1], errQ[2], errQ[3], errQ[4], errQ[5], errQ[6],
//          uErrCnt);
//
//      if (relPP > 0 && relPP < 1
//          && (hra && relPP < metricsRankRange || !hra && relPP >= 1 - metricsRankRange)) {
//        sumAddStdDev += errQ[4];
//        numAddStdDev++;
//      }
//      if (relPP > 0 && relPP < 1
//          && (!hra && relPP < metricsRankRange || hra && relPP >= 1 - metricsRankRange)) {
//        sumRelStdDev += errQ[4] / (hra ? 1 - relPP : relPP);
//        numRelStdDev++;
//      }
//      errQSkArr[pp].reset(); //reset the errQSkArr for next streamLength
//      errHllSkArr[pp].reset(); //reset the errHllSkArr for next streamLength
//    }
//    final int serBytes = sk.smallByteSize();
//
//    // special metrics for capturing accuracy per byte
//    final double avgRelStdDevTimesSize = serBytes * sumRelStdDev / numRelStdDev;
//    final  double avgAddStdDevTimesSize = serBytes * sumAddStdDev / numAddStdDev;
//    job.println(LS + "Avg. relative std. dev. times size: " + avgRelStdDevTimesSize);
//    job.println(     "Avg. additive std. dev. times size: " + avgAddStdDevTimesSize);
//
//    job.println(LS + "Serialization Bytes: " + serBytes);
////    job.println(sk.viewCompactorDetail("%5.0f", false));
//  }
//
//  /**
//   * A trial consists of updating a virgin sketch with a stream of values.
//   * Capture the estimated ranks for all plotPoints and then update the errQSkArr with those
//   * error values.
//   */
//  void doTrial() {
//    sk = TDigest.createDigest(K);
//    if (shuffle) { Shuffle.shuffle(stream); }
//    final int sl = stream.length;
//    for (int i = 0; i < sl; i++) { sk.add(stream[i]); }
//    //get estimated ranks from sketch for all plot points
//    final double[] estRanks = new double[sortedPPValues.length];
//    for (int i = 0; i < sortedPPValues.length; i++) estRanks[i] = sk.cdf(sortedPPValues[i]);
//    //compute errors and update HLL for each plotPoint
//    for (int pp = 0; pp < numPlotPoints; pp++) {
//      final double errorAtPlotPoint = estRanks[pp] - (double)sortedPPAbsRanks[pp] / sl;
//      errQSkArr[pp].update(errorAtPlotPoint); //update each of the errQArr sketches
//      errHllSkArr[pp].update(errorAtPlotPoint); //unique count of error values
//    }
//  }
//
//}
