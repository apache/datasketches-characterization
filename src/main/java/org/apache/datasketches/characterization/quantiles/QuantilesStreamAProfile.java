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

import static java.lang.Math.rint;
import static org.apache.datasketches.characterization.ProfileUtil.buildSplitPointsArr;
import static org.apache.datasketches.quantilescommon.QuantileSearchCriteria.INCLUSIVE;

import java.io.File;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.LineReader;
import org.apache.datasketches.ProcessLine;
import org.apache.datasketches.Properties;
import org.apache.datasketches.UnzipFiles;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

/**
 * @author Lee Rhodes
 */
public class QuantilesStreamAProfile implements JobProfile {

  private Job job;

  //Properties
  private String srcFileName;
  private int reportInterval; //prints number of lines read to console every reportInterval lines.
  private int numRanks; //number of linearly spaced ranks between zero and one.
  private int logBase;
  private int pplb; //number of split-Points Per Log Base.
  private int k; //size of sketch
  private String cdfHdr;
  private String cdfFmt;
  private String pmfHdr;
  private String pmfFmt;

  UpdateDoublesSketch sketch;
  private boolean dataWasZipped = false;
  private double eps = 1e-6;
  private Process proc = new Process();

  //outputs for plotting
  private double minV;
  private double maxV;

  private double[] spArr;  //splitpoints 0, 1 ... ceilPwr2(maxValue), ppo values per octave
  private int numSP;
  private double[] spProbArr;
  private double numItems;

  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
    final Properties prop = job.getProperties();
    //Get Properties
    srcFileName = prop.mustGet("FileName");
    reportInterval = Integer.parseInt(prop.mustGet("ReportInterval"));
    numRanks = Integer.parseInt(prop.mustGet("NumRanks"));
    logBase = Integer.parseInt(prop.mustGet("LogBase"));
    pplb = Integer.parseInt(prop.mustGet("PPLB"));

    k = Integer.parseInt(prop.mustGet("K"));

    cdfHdr = prop.mustGet("CdfHdr").replace("\\t", "\t");
    cdfFmt = prop.mustGet("CdfFmt").replace("\\t", "\t");
    pmfHdr = prop.mustGet("PdfHdr").replace("\\t", "\t");
    pmfFmt = prop.mustGet("PdfFmt").replace("\\t", "\t");

    sketch = DoublesSketch.builder().setK(k).build();

    processInputStream();

    if (dataWasZipped) {
      final File file = new File(srcFileName);
      if (file.exists()) { file.delete(); }
    }
  }

  /**
   * Read file, Print CDF, Print PMF.
   */
  private void processInputStream() {
    checkIfZipped(srcFileName);

    //Read
    job.println("Input Lines Processed: ");
    final LineReader lineReader = new LineReader(srcFileName);

    final long startReadTime_nS = System.nanoTime();
    lineReader.read(0, proc);
    final long readTime_nS = System.nanoTime() - startReadTime_nS;

    //print sketch stats
    job.println(sketch.toString());

    //CDF
    final double[] fracRanks = buildRanksArr(numRanks);
    final long startCdfTime_nS = System.nanoTime();
    final double[] quantiles = sketch.getQuantiles(fracRanks, INCLUSIVE);
    final long cdfTime_nS = System.nanoTime() - startCdfTime_nS;

    job.println("CDF");
    job.println(String.format(cdfHdr, "Index", "Rank", "Quantile"));
    for (int i = 0; i < numRanks; i++) {
      final String s = String.format(cdfFmt, i, fracRanks[i], (int)quantiles[i]);
      job.println(s);
    }
    job.println("");

    //PMF
    minV = sketch.getMinItem();
    maxV = sketch.getMaxItem();
    numItems = sketch.getN();
    spArr = buildSplitPointsArr(minV, maxV, pplb, logBase, eps);
    numSP = spArr.length;
    //PMF
    final long startPmfTime_nS = System.nanoTime();
    spProbArr = sketch.getPMF(spArr);
    final long pmfTime_nS = System.nanoTime() - startPmfTime_nS;

    printPMF();
    printTimes(readTime_nS, cdfTime_nS, pmfTime_nS);
  }

  private void printPMF() {
    job.println("PMF");
    job.println(String.format(pmfHdr, "Index", "Quantile", "Mass"));
    for (int i = 0; i < numSP; i++) {
      job.println(String.format(pmfFmt, i, spArr[i], spProbArr[i] * numItems));
    }
  }

  private void printTimes(final long readTime_nS, final long cdfTime_nS, final long pmfTime_nS) {
    final double readTime_S = readTime_nS / 1E9;
    job.println("");
    job.println(String.format("ReadTime_Sec  :\t%10.3f", readTime_S));
    job.println(String.format("ReadRate/Sec  :\t%,10.0f", numItems / readTime_S));
    job.println(String.format("CdfTime_mSec  :\t%10.3f", cdfTime_nS / 1E6));
    job.println(String.format("Cdf/Point_nSec:\t%10.3f", (double)cdfTime_nS / numRanks));
    job.println(String.format("PmfTime_mSec  :\t%10.3f", pmfTime_nS / 1E6));
    job.println(String.format("Pmf/Point_nSec:\t%10.3f", (double)pmfTime_nS / numSP));
  }

  /**
   * Compute the ranks array.
   * @param numRanks the number of evenly-spaced rank values including 0 and 1.0.
   * @return the ranks array
   */
  private static double[] buildRanksArr(final int numRanks) {
    final int numRM1 = numRanks - 1;
    final double[] fractions = new double[numRanks];
    final double delta = 1.0 / numRM1;
    double d = 0.0;
    for (int i = 0; i < numRanks; i++) {
      fractions[i] = d;
      d += delta;
      d = rint(d * numRM1) / numRM1;
    }
    return fractions;
  }

  private void checkIfZipped(final String srcFileName) {
    final File file = new File(srcFileName);
    if (!file.exists()) {
      final String srcZipFile = srcFileName + ".zip";
      final File zipFile = new File(srcZipFile);
      if (!zipFile.exists()) {
        throw new IllegalArgumentException("Neither file nor zipFile exists.");
      }
      final String parent = zipFile.getParent();
      job.println("Unzipping data file: " + srcZipFile + "...");
      UnzipFiles.unzip(srcZipFile, parent);
      if (!zipFile.exists()) {
        throw new IllegalArgumentException("Unsuccessful Unzip.");
      }
      job.println(srcZipFile + " unzipped!");
      dataWasZipped = true;
    }
  }

  //JobProfile
  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

  // Callback
  class Process implements ProcessLine {
    int n = 0;

    @Override
    public void process(final String strArr0, final int lineNo) {
      if (lineNo % reportInterval == 0) {
        job.println("" + lineNo);
      }
      final long v = Long.parseLong(strArr0);
      sketch.update(v);
      n++;
    }
  }

}
