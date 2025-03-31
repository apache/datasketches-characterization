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

import static org.apache.datasketches.GaussianRanks.GAUSSIANS_3SD;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.EXCLUSIVE;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;
import org.apache.datasketches.characterization.QuantilesAccuracyStats;
import org.apache.datasketches.characterization.Shuffle;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.kll.KllSketch;
import org.apache.datasketches.kll.KllSketch.SketchType;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

/**
 * @author Lee Rhodes
 */
public class KllFloatsSketchWeightedRankGaussianAccuracyProfile implements JobProfile {
  private static final DefaultMemoryRequestServer memReqSvr = new DefaultMemoryRequestServer();
  private static final String[] columnLabels =
    {"Row", "Quantile", "NormRank", "-3SD","-2SD", "-1SD", "Med", "+1SD", "+2SD", "+3SD"};
  private static final String sFmt =
    "%3s\t%8s\t%8s\t%4s\t%4s\t%4s\t%4s\t%4s\t%4s\t%4s\n";
  private static final String fFmt =
    "%5d\t%14.0f\t%14.10f\t" //rPP, Value, NormRank
  + "%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\t%14.10f\n"; //-3sd to +3sd

  private Job job;
  private Properties props;

  //PROPERTIES to be derived from config file
  //Trial parameters
  private int numTrials; //num of Trials per plotPoint
  private int errorSkLgK; //size of the error quantiles sketches

  //Target sketch configuration & error analysis
  private int k;
  private QuantileSearchCriteria criteria;
  //private boolean useGetRanks;
  private boolean direct;
  private boolean weightedUpdate; //For determining the update method
  private SketchType sketchType;
  //END FROM PROPERTIES

  //DERIVED
  private KllFloatsSketch sk;
  private int numSteps;
  private int stepSize;
  private int streamLen;
  private long adjRank;

  //The array of Gaussian quantiles for +/- StdDev error analysis
  private double[] gRanks;
  //The tracking array for true values,
  private QuantilesAccuracyStats[] qStatsArr;

  @Override
  public void start(final Job job) {
    this.job = job;
    this.props = job.getProperties();
    extractProperties();
    configureQuantilesAccuracyStatsArray();
    gRanks = createGaussianQuantilesArray();
    configureSketch();
    doTrials();
  }

  private void extractProperties() {
    final int lgStepSize = Integer.parseInt(props.mustGet("LgStepSize"));
    stepSize = 1 << lgStepSize;
    final int lgNumSteps = Integer.parseInt(props.mustGet("LgNumSteps"));
    numSteps = 1 << lgNumSteps;
    streamLen = 1 << (lgStepSize + lgNumSteps);

    //numTrials & error quantiles sketch config
    numTrials = 1 << Integer.parseInt(props.mustGet("LgTrials"));
    errorSkLgK = Integer.parseInt(props.mustGet("ErrSkLgK"));
    adjRank = (criteria == EXCLUSIVE) ? 1L : 0;

    //Target sketch config
    k = Integer.parseInt(props.mustGet("K"));
    criteria = props.mustGet("Criteria").equalsIgnoreCase("INCLUSIVE") ? INCLUSIVE : EXCLUSIVE;
    //useGetRanks = Boolean.parseBoolean(props.mustGet("UseGetRanks"));
    direct = Boolean.parseBoolean(props.mustGet("Direct"));
    weightedUpdate = Boolean.parseBoolean(props.mustGet("WeightedUpdate"));

    final String dataType = props.mustGet("dataType");
    if ( dataType.equalsIgnoreCase("double")) { sketchType = SketchType.DOUBLES_SKETCH; }
    else if (dataType.equalsIgnoreCase("float")) { sketchType = SketchType.FLOATS_SKETCH; }
    //else { sketchType = SketchType.ITEMS_SKETCH; }
    else { throw new IllegalArgumentException("Unknown data type."); }
  }

  void configureQuantilesAccuracyStatsArray() { //initially monotonic
    qStatsArr = new QuantilesAccuracyStats[numSteps];
    for (int i = 0; i < numSteps; i++) {
      final double trueQuantile = stepSize * (i + 1);
      qStatsArr[i] = new QuantilesAccuracyStats(errorSkLgK, i, trueQuantile);
      final long naturalRank = (long)trueQuantile;//only true for monotonic, non-random
      qStatsArr[i].naturalRank = naturalRank;
      qStatsArr[i].normRank = (naturalRank - adjRank) / (double) streamLen;
    }
  }

  //create the array of gaussian quantiles corresponding to +/- 1,2,3 standard deviations
  double[] createGaussianQuantilesArray() {
    final double[] gR = new double[GAUSSIANS_3SD.length - 2]; //omit 0.0 and 1.0
    for (int i = 1; i < GAUSSIANS_3SD.length - 1; i++) {
      gR[i - 1] = GAUSSIANS_3SD[i];
    }
    return gR;
  }

  void configureSketch() {
    if (direct) {
      final int memBytes = KllSketch.getMaxSerializedSizeBytes(k, streamLen, sketchType, true);
      final WritableMemory wmem = WritableMemory.allocate(memBytes);
      sk = KllFloatsSketch.newDirectInstance(k, wmem, memReqSvr);
    } else {
      sk = KllFloatsSketch.newHeapInstance(k);
    }
  }

  void doTrials() {
    job.println("");
    job.printfData(sFmt, (Object[])columnLabels);

    //Do numTrials for all plot points
    for (int t = 0; t < numTrials; t++) {
      sk.reset();
      doTrial();
    }

    //at this point each of the qStatsArr sketches has a distribution of error from the trials
    QuantilesAccuracyStats.sortByQuantile(qStatsArr);
    for (int i = 0 ; i < numSteps; i++) {
      //for each of the numErrDistRanks distributions extract the sd quantiles
      final double[] errQ = qStatsArr[i].qsk.getQuantiles(gRanks); //get error values at the Gaussian ranks

      //Plot the row. We ignore quantiles collected at 0 and 1.0.
      final int numPP = i + 1;
      job.printfData(fFmt, numPP, qStatsArr[i].quantile, qStatsArr[i].normRank,
          errQ[0], errQ[1], errQ[2], errQ[3], errQ[4], errQ[5], errQ[6]);
      qStatsArr[i].qsk.reset(); //reset the errQSkArr for next set if trials
    }

    job.println(sk.toString(true, false));
  }

  /**
   * A trial consists of updating a virgin sketch with a shuffled stream of streamLength values.
   * We capture the estimated ranks for all plotPoints and then update the errQSkArr with those
   * error values.
   */
  void doTrial() {
    Shuffle.shuffle(qStatsArr);
    if (weightedUpdate) {
      for (int i = 0; i < numSteps; i++) {
        sk.update((float)qStatsArr[i].quantile, stepSize);
      }
    } else { //single
      for (int i = 0; i < numSteps; i++) {
        for (int j = 0; j < stepSize; j++) {
          sk.update((float)qStatsArr[i].quantile);
        }
      }
    }

    //get estimated ranks from sketch for all steps

    double estNormalizedRank;
    for (int i = 0; i < numSteps; i++) {
      qStatsArr[i].estNormRank = estNormalizedRank = sk.getRank((float)qStatsArr[i].quantile, criteria);
      final double errorAtPlotPoint = estNormalizedRank - qStatsArr[i].normRank;
      qStatsArr[i].qsk.update(errorAtPlotPoint); //update each of the errQArr sketches
    }
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

}
