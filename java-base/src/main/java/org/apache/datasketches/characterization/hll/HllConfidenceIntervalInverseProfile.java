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

package org.apache.datasketches.characterization.hll;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.datasketches.GaussianRanks.GAUSSIANS_3SD;
import static org.apache.datasketches.common.Util.milliSecToString;
import static org.apache.datasketches.common.Util.pwr2SeriesNext;

import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

/**
 * @author Lee Rhodes
 */
public class HllConfidenceIntervalInverseProfile implements JobProfile {
  static final int FRACTIONS_3SD_LEN = GAUSSIANS_3SD.length;
  Job job;
  PrintWriter pw;
  public Properties prop;
  public long vIn = 0;
  // Range of N
  int minSrcN;
  int maxSrcN;
  // Range of target values of estimates
  int centerTgtEst;
  int deltaTgtEst;
  int minTgtEst;
  int maxTgtEst;
  // Trials profile
  int minT;
  int maxT;
  int tPPO;

  int lgQK; //size of quantile sketches

  // HLL Sketch profile - defaults
  int lgK;
  boolean hllDirect = false;
  boolean useComposite = false;
  TgtHllType type = TgtHllType.HLL_4;
  boolean interData = true;
  EstimateStats[] estStatsArr = null;
  private HllSketch sketch = null;

  //Other stats:
  int srcNlen;
  int tgtEstArrLen;
  int maxTgtHits = 0;
  int minTgtHits = Integer.MAX_VALUE;
  int totalTgtHits = 0;
  double[] sumRanksArr = new double[FRACTIONS_3SD_LEN];
  UpdateDoublesSketch qNinTgtEstRange; //distribution of N values in the target estimate range

  @Override
  public void start(final Job job) {
    this.job = job;
    pw = job.getPrintWriter();
    prop = job.getProperties();
    minSrcN = Integer.parseInt(prop.mustGet("MinSrcN"));
    maxSrcN = Integer.parseInt(prop.mustGet("MaxSrcN"));
    centerTgtEst = Integer.parseInt(prop.mustGet("CenterTgtEst"));
    deltaTgtEst = Integer.parseInt(prop.mustGet("DeltaTgtEst"));
    minTgtEst = centerTgtEst - deltaTgtEst;
    maxTgtEst = centerTgtEst + deltaTgtEst;
    tgtEstArrLen = maxTgtEst - minTgtEst + 1;
    minT = 1 << Integer.parseInt(prop.mustGet("Trials_lgMinT"));
    maxT = 1 << Integer.parseInt(prop.mustGet("Trials_lgMaxT"));
    tPPO = Integer.parseInt(prop.mustGet("Trials_TPPO"));
    lgQK = Integer.parseInt(prop.mustGet("Trials_lgQK"));
    lgK = Integer.parseInt(prop.mustGet("LgK"));
    srcNlen = maxSrcN - minSrcN + 1;
    estStatsArr = buildEstimateStatsArray(minTgtEst, maxTgtEst, lgQK);
    qNinTgtEstRange = new DoublesSketchBuilder().setK(1 << lgQK).build();
    configureSketch();
    doTrials();
    shutdown();
    cleanup();
  }

  @Override
  public void shutdown() {
    //At the very end
    job.println("");
    job.println(prop.extractKvPairs(LS));
    job.flush();
  }

  @Override
  public void cleanup() {}

  public void configureSketch() {
    //Configure Sketch
    hllDirect = Boolean.parseBoolean(prop.mustGet("HLL_direct"));
    useComposite = Boolean.parseBoolean(prop.mustGet("HLL_useComposite"));

    final TgtHllType tgtHllType;
    final String type = prop.mustGet("HLL_tgtHllType");
    if (type.equalsIgnoreCase("HLL4")) { tgtHllType = TgtHllType.HLL_4; }
    else if (type.equalsIgnoreCase("HLL6")) { tgtHllType = TgtHllType.HLL_6; }
    else { tgtHllType = TgtHllType.HLL_8; }

    if (hllDirect) {
      final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, tgtHllType);
      final WritableMemory wmem = WritableMemory.allocate(bytes);
      sketch = new HllSketch(lgK, tgtHllType, wmem);
    } else {
      sketch = new HllSketch(lgK, tgtHllType);
    }
  }

  public void doTrial() {
    for (int n = minSrcN; n <= maxSrcN; n++) { //srcN range: from 1300 to 17000
      sketch.reset();
      for (long u = 0; u < n; u++) { //update sketch with n uniques
        sketch.update(++vIn);
      }
      final double rawEst = useComposite ? sketch.getCompositeEstimate() : sketch.getEstimate();
      final int est = (int)Math.round(rawEst);
      if (est >= minTgtEst && est <= maxTgtEst) { //est range
        final int deltaEst = est - minTgtEst;
        final EstimateStats q = estStatsArr[deltaEst];
        if (q.estimate != est) {
          throw new IllegalArgumentException("q.estimate: " + q.estimate + " != est: " + est);
        }
        q.update(n); //distribution of srcN values in the target est bin (row)
        qNinTgtEstRange.update(n); //distribution of srcN values in the target estimate range
      }
    } // end scan of N
  }

  private void doTrials() {
    //This will generate a table of data for each intermediate Trials point
    int lastT = 0;
    while (lastT < maxT) {
      final int nextT = lastT == 0 ? minT : (int)pwr2SeriesNext(tPPO, lastT);
      final int delta = nextT - lastT;
      for (int i = 0; i < delta; i++) {
        doTrial();
      }
      lastT = nextT;
      final StringBuilder sb = new StringBuilder();
      if (nextT < maxT) { // intermediate
        if (interData) {
          processTrialsSet(estStatsArr, lastT, sb);
          job.println(sb.toString());
        }
      } else { //done
        processTrialsSet(estStatsArr, lastT, sb);
        job.println(sb.toString());
      }
      //printed at the end of a trials set
      final long currentTime_mS = System.currentTimeMillis();
      final long cumTime_mS = currentTime_mS - job.getStartTime();
      job.println("Cum Time               : " + milliSecToString(cumTime_mS));
      final double timePerTrial_mS = cumTime_mS * 1.0 / lastT;
      final String tpt_ms = String.format("%.3f", timePerTrial_mS);
      job.println("Time Per Trial, mSec   : " + tpt_ms);

      job.println("Date Time              : "
          + job.getReadableDateString(currentTime_mS));

      final long timeToComplete_mS = (long)(timePerTrial_mS * (maxT - lastT));
      job.println("Est Time to Complete   : " + milliSecToString(timeToComplete_mS));
      job.println("Est Time at Completion : "
          + job.getReadableDateString(timeToComplete_mS + currentTime_mS));
      job.println("");
      job.flush();
    }
  }

  private void processTrialsSet( //process cumulative trials so far
      final EstimateStats[] estStatsArr,
      final int trials,
      final StringBuilder sb
      ) {
    //Reset for this Trials Set
    final int totalEsts = trials * srcNlen;
    minTgtHits = Integer.MAX_VALUE;
    Arrays.fill(sumRanksArr, 0.0);
    sb.setLength(0);
    sb.append(getHeader()).append(LS);

    for (int pt = 0; pt < tgtEstArrLen; pt++) { //output each target est bin per row
      final DoublesSketch qsk = estStatsArr[pt].qskN;
      final int est = estStatsArr[pt].estimate;
      final int hits = (int) qsk.getN();
      maxTgtHits = max(maxTgtHits, hits);
      minTgtHits = min(minTgtHits, hits);
      totalTgtHits += hits;

      //start output
      sb.append(est).append(TAB);
      sb.append(hits).append(TAB);

      //output quantiles for each target est bin on the row
      final double[] quants = qsk.getQuantiles(GAUSSIANS_3SD);
      if (hits > 0) {
        for (int i = 0; i < FRACTIONS_3SD_LEN; i++) {
          final double relV = quants[i] / est - 1.0;
          sumRanksArr[i] += relV;
          sb.append(relV).append(TAB);
        }
      } else {
        for (int i = 0; i < FRACTIONS_3SD_LEN; i++) { sb.append("-").append(TAB); }
      }
      sb.append(LS);
    }
    //output summary statistics for trials set so far
    sb.append(LS);
    sb.append("Summary quantiles of N over all target estimates").append(LS);
    sb.append(centerTgtEst).append(TAB);
    sb.append(totalTgtHits).append(TAB);
    final double[] qNarr = qNinTgtEstRange.getQuantiles(GAUSSIANS_3SD);
    for (int i = 0; i < FRACTIONS_3SD_LEN; i++) {
      final String relV = totalTgtHits > 0 ? Double.toString(qNarr[i] / centerTgtEst - 1.0) : "-";
      sb.append(relV).append(TAB);
    }
    sb.append(LS);
    sb.append("Ave").append(TAB).append(TAB);
    for (int i = 0; i < FRACTIONS_3SD_LEN; i++) {
      final double relV = sumRanksArr[i] / tgtEstArrLen;
      sb.append(relV).append(TAB);
    }

    sb.append(LS + LS);
    sb.append(getSumHeader()).append(LS);
    sb.append(trials).append(TAB);
    sb.append((int)qNinTgtEstRange.getMinItem()).append(TAB);
    sb.append((int)qNinTgtEstRange.getQuantile(.5)).append(TAB);
    sb.append((int)qNinTgtEstRange.getMaxItem()).append(TAB);
    sb.append(minTgtHits).append(TAB);
    sb.append(maxTgtHits).append(TAB);
    sb.append(totalTgtHits).append(TAB);
    sb.append(totalEsts).append(TAB);
    sb.append(vIn);
    sb.append(LS);
  }

  private static String getSumHeader() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Trials").append(TAB);
    sb.append("MinSrcN").append(TAB);
    sb.append("MedianSrcN").append(TAB);
    sb.append("MaxSrcN").append(TAB);
    sb.append("MinHits").append(TAB);
    sb.append("MaxHits").append(TAB);
    sb.append("TotalHits").append(TAB);
    sb.append("TotalEsts").append(TAB);
    sb.append("TotalUpdates");
    return sb.toString();
  }

  private static String getHeader() {
    final StringBuilder sb = new StringBuilder();
    sb.append("Est").append(TAB);
    sb.append("Hits").append(TAB);

    //Quantiles
    sb.append("Min").append(TAB);
    sb.append("Q(.00135)").append(TAB);
    sb.append("Q(.02275)").append(TAB);
    sb.append("Q(.15866)").append(TAB);
    sb.append("Q(.5)").append(TAB);
    sb.append("Q(.84134)").append(TAB);
    sb.append("Q(.97725)").append(TAB);
    sb.append("Q(.99865)").append(TAB);
    sb.append("Max");
    return sb.toString();
  }

  public class EstimateStats {
    public UpdateDoublesSketch qskN;
    public int estimate;

    public EstimateStats(final int qK, final int estimate) {
      qskN = new DoublesSketchBuilder().setK(qK).build(); //Quantiles of N
      this.estimate = estimate;
    }

    public void update(final double n) {
      qskN.update(n);
    }
  }

  private final EstimateStats[] buildEstimateStatsArray(
      final int minTgtEst, final int maxTgtEst, final int lgQK) {
    final EstimateStats[] estStatsArr = new EstimateStats[tgtEstArrLen];
    for (int i = minTgtEst; i <= maxTgtEst; i++) {
      estStatsArr[i - minTgtEst] = new EstimateStats(1 << lgQK, i);
    }
    return estStatsArr;
  }

}
