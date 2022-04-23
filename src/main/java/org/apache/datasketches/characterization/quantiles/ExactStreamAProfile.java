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

import static org.apache.datasketches.characterization.ProfileUtil.buildSplitPointsArr;
import static org.apache.datasketches.characterization.ProfileUtil.checkMonotonic;

import java.io.File;
import java.util.Arrays;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.LineReader;
import org.apache.datasketches.ProcessLine;
import org.apache.datasketches.Properties;
import org.apache.datasketches.UnzipFiles;
//import org.testng.annotations.Test;

/**
 * Computes exact quantiles using brute-force methods.
 *
 * @author Lee Rhodes
 */
public class ExactStreamAProfile implements JobProfile {

  private Job job;

  //Properties
  private String srcFileName;
  private int reportInterval; //prints number of lines read to console every reportInterval lines.
  private int numRanks; //number of linearly spaced ranks between zero and one.
  private int pplb; //number of split-Points Per Log Base.
  private double lb; //Log Base
  private String cdfHdr;
  private String cdfFmt;
  private String pmfHdr;
  private String pmfFmt;

  //processing input array
  private boolean dataWasZipped = false;
  private static final int numItems = 263078000;
  private int[] dataArr = new int[numItems];
  private Process proc = new Process();

  //outputs for plotting
  private double minV;
  private double maxV;

  private double[] spArr;  //splitpoints 0, 1 ... ceilPwr2(maxValue), ppo values per octave
  private int numSP;
  private long[] spCounts;
  private double eps = 1E-6;



  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
    final Properties prop = job.getProperties();

    //Get Properties
    srcFileName = prop.mustGet("FileName");
    reportInterval = Integer.parseInt(prop.mustGet("ReportInterval"));
    numRanks = Integer.parseInt(prop.mustGet("NumRanks"));
    pplb = Integer.parseInt(prop.mustGet("PPLB"));
    lb = Double.parseDouble(prop.mustGet("LogBase"));

    cdfHdr = prop.mustGet("CdfHdr").replace("\\t", "\t");
    cdfFmt = prop.mustGet("CdfFmt").replace("\\t", "\t");
    pmfHdr = prop.mustGet("PdfHdr").replace("\\t", "\t");
    pmfFmt = prop.mustGet("PdfFmt").replace("\\t", "\t");

    processInputStream();

    if (dataWasZipped) { //after we are done
      final File file = new File(srcFileName);
      if (file.exists()) { file.delete(); }
    }
  }

  /**
   * Read file, Print CDF, Print PMF.
   */
  private void processInputStream() {
    checkIfZipped(srcFileName);
    long startTime_nS;

    //Read
    job.println("Input Lines Processed: ");
    final LineReader lineReader = new LineReader(srcFileName);
    startTime_nS = System.nanoTime();
    lineReader.read(0, proc);
    final long readTime_nS = System.nanoTime() - startTime_nS;
    assert proc.n == numItems;

    //Sort
    job.println("Sort input data");
    startTime_nS = System.nanoTime();
    Arrays.sort(dataArr);
    final long sortTime_nS = System.nanoTime() - startTime_nS;
    job.println("Sort done");

    //Compute CDF & PMF
    minV = dataArr[0];
    maxV = dataArr[numItems - 1];
    assert maxV > minV;

    //outputs
    //ranksArr and quantilesArr already initialized
    spArr = buildSplitPointsArr(minV, maxV, pplb, lb, eps);
    checkMonotonic(spArr);
    numSP = spArr.length;
    spCounts = new long[numSP];

    job.println("");
    printExactPercentiles(dataArr);
    job.println("End Percentiles");
    job.println("");

    job.println("Process PMF");
    startTime_nS = System.nanoTime();
    processExactPmf(dataArr, spArr, spCounts);//, ranksArr, quantilesArr, cdfIdxArr);
    final long processTime_nS = System.nanoTime() - startTime_nS;
    job.println("End Process PMF");
    job.println("");
    printPMF();
    job.println("");
    printTimes(readTime_nS, sortTime_nS, processTime_nS);
  }

  private void printExactPercentiles(final int[] sortedArr) {
    final int len = sortedArr.length;
    job.println("Percentiles");
    job.println(String.format(cdfHdr, "Num", "Rank", "Index", "Value"));
    int index, v;
    for (int i = 0; i < 100; i++) {
      final double fracRank = i * .01;
      index = (int) (fracRank * len);
      v = sortedArr[index];
      job.println(String.format(cdfFmt, i, fracRank, index, v));
    }
    index = numItems - 1; //max value
    v = sortedArr[index];
    job.println(String.format(cdfFmt, 101, 1.0, index, v));
  }

  private static void processExactPmf(final int[] sortedArr, final double[] spArr,
      final long[] spCounts) {
    int spIdx = 0;
    final int dataLen = sortedArr.length;
    final int spLen = spArr.length;
    for (int i = 0; i < dataLen; i++) {
      final double v = sortedArr[i];
      if (v < spArr[spIdx]) {
        spCounts[spIdx]++;
      } else {
        while (spIdx + 1 < spLen && v >= spArr[spIdx]) {
          spIdx++;
        }
        spCounts[spIdx]++;
      }
    }
  }

  //@Test
  public void checkProcessArr() {
    final int[] data = new int[10000];
    for (int i = 0; i < data.length; i++) { data[i] = i; }
    final double[] spArr = buildSplitPointsArr(0, 9999, 1, 2.0, 1E-6);
    final long[] spCounts = new long[spArr.length];
    processExactPmf(data, spArr, spCounts);
    System.out.println("Done");
  }

  private void printPMF() {
    job.println("PMF");
    job.println(String.format(pmfHdr, "Index", "Quantile", "Mass"));
    final int numSP = spArr.length;
    for (int i = 0; i < numSP; i++) {
      job.println(String.format(pmfFmt, i, spArr[i], spCounts[i]));
    }
  }

  private void printTimes(final long readTime_nS, final long sortTime_nS, final long processTime_nS) {
    final double readTime_S = readTime_nS / 1E9;
    job.println(String.format("ReadTime_Sec  :\t%10.3f", readTime_S));
    job.println(String.format("ReadRate/Sec  :\t%,10.0f", proc.n / readTime_S));
    job.println(String.format("SortTime_nS   :\t%,10d", sortTime_nS));
    job.println(String.format("ProcessTime_mSec  :\t%10.3f", processTime_nS / 1E6));
    job.println(String.format("PT/Point_nSec:\t%10.3f", (double)processTime_nS / numRanks));
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
      dataArr[n++] = Integer.parseInt(strArr0);
    }
  }

}
