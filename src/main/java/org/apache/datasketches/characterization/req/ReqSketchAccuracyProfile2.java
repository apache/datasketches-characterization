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

import static org.apache.datasketches.GaussianRanks.GAUSSIANS_3SD;
import static org.apache.datasketches.Util.evenlySpacedFloats;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.req.StreamMaker.Pattern;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;
import org.apache.datasketches.req.ReqDebugImpl;
import org.apache.datasketches.req.ReqSketch;
import org.apache.datasketches.req.ReqSketchBuilder;

/**
 * @author Lee Rhodes
 */
public class ReqSketchAccuracyProfile2 implements JobProfile {
  private Job job;
  private Properties prop;
  Pattern pattern;

  //PROPERTIES
  //plotting & x-axis configuration
  private int lgSL;
  private int numPlotPoints;
  private int stdDev;

  //Patterns
  private int offset; //Stream offset, 0 or 1
//  private int[] advSeq1 = new int[3];
//  private int[] advSeq2 = new int[3];
//  private int[] advSeq3 = new int[3];

  private int errQSkLgK;

  //TargetSketch config & error analysis
  private int K;
  private boolean hra;
  private boolean ltEq;
  private org.apache.datasketches.req.ReqDebugImpl reqDebugImpl = null;

  //DERIVED INTERNAL globals
  private ReqSketch sk;
  private int N;

  //The array of Gaussian quantiles for +/- StdDev error analysis
  private double[] gRanks;
  private UpdateDoublesSketch[] errQSkArr;

  //Specific to the stream
  private StreamMaker streamMaker;
  private TrueFloatRanks trueRanks;
  private float[] sortedPPValues;
  private int[] sortedPPIndices;
  private int[] sortedPPAbsRanks;

  private final String[] columnLabels =
      { "PP", "Value", "TrueRanks",
        "-3SD","-2SD", "-1SD", "Med", "+1SD", "+2SD", "+3SD",
        "1LB", "1UB" };
  private final String sFmt =
        "%2s\t%5s\t%9s\t"
      + "%4s\t%4s\t%4s\t%3s\t%4s\t%4s\t%4s\t"
      + "%3s\t%3s\n";
  private final String fFmt =
      "%5d\t%,14.0f\t%14.10f\t" //nPP, Value, Rank
    + "%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t" //-3sd to +3sd
    + "%14.10f\t%14.10f\n"; //1lb, 1ub

  //JobProfile interface
  @Override
  public void start(final Job job) {
    this.job = job;
    prop = job.getProperties();
    extractProperties();
    configureSketch();
    configureStream();
    configurePlotPoints();
    doStreamLength(N);
  }

  @Override
  public void shutdown() { }

  @Override
  public void cleanup() { }
  //end JobProfile

  private void extractProperties() {
    //plotting & x-axis configuration
    lgSL = Integer.parseInt(prop.mustGet("LgSL"));
    numPlotPoints = Integer.parseInt(prop.mustGet("NumPlotPoints"));
    stdDev = Integer.parseInt(prop.mustGet("StdDev"));
    //Patterns
    pattern = Pattern.valueOf(prop.mustGet("Pattern"));
    offset = Integer.parseInt(prop.mustGet("Offset"));
//    String[] sarr = prop.mustGet("AdvSeq1").split(",", 3);
//    for (int i = 0; i < sarr.length; i++) { advSeq1[i] = Integer.parseInt(sarr[i]); }
//    sarr = prop.mustGet("AdvSeq2").split(",", 3);
//    for (int i = 0; i < sarr.length; i++) { advSeq2[i] = Integer.parseInt(sarr[i]); }
//    sarr = prop.mustGet("AdvSeq3").split(",", 3);
//    for (int i = 0; i < sarr.length; i++) { advSeq3[i] = Integer.parseInt(sarr[i]); }

    // error quantiles & HLL sketch config
    errQSkLgK = Integer.parseInt(prop.mustGet("ErrQSkLgK"));
    //plotting & x-axis config

    //Target sketch config
    K = Integer.parseInt(prop.mustGet("K"));
    hra = Boolean.parseBoolean(prop.mustGet("HRA"));
    ltEq = Boolean.parseBoolean(prop.mustGet("LtEq"));
    final String reqDebugLevel = prop.get("ReqDebugLevel");
    final String reqDebugFmt = prop.get("ReqDebugFmt");
    if (reqDebugLevel != null) {
      final int level = Integer.parseInt(reqDebugLevel);
      reqDebugImpl = new ReqDebugImpl(level, reqDebugFmt);
    }
  }

  private void configureSketch() {
    final ReqSketchBuilder bldr = ReqSketch.builder();
    bldr.setK(K).setHighRankAccuracy(hra);
    if (reqDebugImpl != null) { bldr.setReqDebug(reqDebugImpl); }
    sk = bldr.build();
    sk.setLessThanOrEqual(ltEq);
  }

  private void configureStream() {
    N = 1 << lgSL;
    streamMaker = new StreamMaker();
    final float[] stream = streamMaker.makeStream(N, pattern, offset);
    if (ltEq) {
      trueRanks = new TrueFloatRanks(stream, true);
    } else {
      trueRanks = new TrueFloatRanks(stream, false);
    }
  }

  private void configurePlotPoints() {
    sortedPPIndices = new int[numPlotPoints];
    sortedPPAbsRanks = new int[numPlotPoints];
    sortedPPValues = new float[numPlotPoints];
    final int[] sortedAbsRanks = trueRanks.getSortedAbsRanks();
    final float[] sortedStream = trueRanks.getSortedStream();
    final int minIdx = (int)Math.round((double)(N - 1) / numPlotPoints);
    final float[] temp = evenlySpacedFloats(minIdx, N - 1, numPlotPoints); //indices

    for (int pp = 0; pp < numPlotPoints; pp++) {
      final int idx = Math.round(temp[pp]);
      sortedPPIndices[pp] = idx;
      sortedPPAbsRanks[pp] = sortedAbsRanks[idx];
      sortedPPValues[pp] = sortedStream[idx];
    }

    //configure the error quantiles array
    errQSkArr = new UpdateDoublesSketch[numPlotPoints];
    final DoublesSketchBuilder builder = DoublesSketch.builder().setK(1 << errQSkLgK);
    for (int pp = 0; pp < numPlotPoints; pp++) {
      errQSkArr[pp] = builder.build();
    }
    gRanks = new double[GAUSSIANS_3SD.length - 2]; //omit 0.0 and 1.0
    for (int i = 1; i < GAUSSIANS_3SD.length - 1; i++) {
      gRanks[i - 1] = GAUSSIANS_3SD[i];
    }
  }

  private void doStreamLength(final int streamLength) {
    job.println(LS + "Stream Length: " + streamLength );
    job.printfData(sFmt, (Object[])columnLabels);

    doTrial();

    //at this point each of the errQSkArr sketches has a distribution of error
    for (int pp = 0 ; pp < numPlotPoints; pp++) {
      final double tr = (double)sortedPPAbsRanks[pp] / N;
      final float v = sortedPPValues[pp];
      final double rlb = sk.getRankLowerBound(tr, stdDev) - tr;
      final double rub = sk.getRankUpperBound(tr, stdDev) - tr;

      //for each of the numErrDistRanks distributions extract the sd quantiles
      final double[] errQ = errQSkArr[pp].getQuantiles(gRanks); //get error values at the Gaussian ranks
      if (errQ != null) {
      //Plot the row.
      job.printfData(fFmt, pp + 1, v, tr,
          errQ[0], errQ[1], errQ[2], errQ[3], errQ[4], errQ[5], errQ[6],
          rlb, rub);
      }
      errQSkArr[pp].reset(); //reset the errQSkArr for next streamLength
    }
    job.println(LS + "Serialization Bytes: " + sk.getSerializationBytes());
    job.println(sk.viewCompactorDetail("%5.0f", false));
  }

  void doTrial() { //for all plot points
    sk.reset();
    final float[] stream = trueRanks.getStream();
    for (int i = 0; i < N; i++) { sk.update(stream[i]); }

    final float[] sortedStream = trueRanks.getSortedStream();
    final int[] sortedAbsRanks = trueRanks.getSortedAbsRanks();

    int pp = 0;
    int ppAbsIdx = sortedPPIndices[pp];
    UpdateDoublesSketch qSk = errQSkArr[pp];
    for (int idx = 0; idx < N; idx++) {
      final double skRank = sk.getRank(sortedStream[idx]);
      final double trueRank = (double)sortedAbsRanks[idx] / N;
      if (idx <= ppAbsIdx) {
        final double rErr = skRank - trueRank;
        qSk.update(rErr);
      } else {
        if (++pp < numPlotPoints) {
          ppAbsIdx = sortedPPIndices[pp];
          qSk = errQSkArr[pp];
          final double rErr = skRank - trueRank;
          qSk.update(rErr);
        } else { break; }
      }
    }
  }
}
