/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.quantiles;

import static com.yahoo.sketches.Util.ceilingPowerOf2double;
import static com.yahoo.sketches.Util.log2;
import static com.yahoo.sketches.Util.pwr2LawNextDouble;
import static java.lang.Math.rint;
import static java.lang.Math.round;

import java.io.File;
import java.util.Arrays;

import org.testng.annotations.Test;

import com.yahoo.sketches.LineReader;
import com.yahoo.sketches.ProcessLine;
import com.yahoo.sketches.UnzipFiles;
import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.characterization.Properties;

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
  private int ppo; //number of split-points per Octave.
  private String cdfHdr;
  private String cdfFmt;
  private String pmfHdr;
  private String pmfFmt;

  //processing input array
  private boolean dataWasZipped = false;
  private static final int numItems = 263078000;
  private int[] dataArr = new int[numItems];

  //outputs for plotting
  private double minV;
  private double maxV;
  private double[] ranksArr; // 0, .01, .02, ...1.0 //101 ranks
  private double[] quantilesArr; //101 values
  private int[] cdfIdxArr; //101 values

  private double[] spArr;  //splitpoints 1 ... ceilPwr2(maxValue), ppo values per octave
  private int numSP;
  private long[] spCounts;  //
  private Process proc = new Process();


  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
    final Properties prop = job.getProperties();

    //Get Properties
    srcFileName = prop.mustGet("FileName");
    reportInterval = Integer.parseInt(prop.mustGet("ReportInterval"));
    numRanks = Integer.parseInt(prop.mustGet("NumRanks"));
    ppo = Integer.parseInt(prop.mustGet("PPO"));

    cdfHdr = prop.mustGet("CdfHdr").replace("\\t", "\t");
    cdfFmt = prop.mustGet("CdfFmt").replace("\\t", "\t");
    pmfHdr = prop.mustGet("PdfHdr").replace("\\t", "\t");
    pmfFmt = prop.mustGet("PdfFmt").replace("\\t", "\t");

    ranksArr = buildRanksArr(numRanks);
    quantilesArr = new double[numRanks];
    cdfIdxArr = new int[numRanks];
    checkMonotonic(ranksArr);

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
    println("Input Lines Processed: ");
    final LineReader lineReader = new LineReader(srcFileName);
    startTime_nS = System.nanoTime();
    lineReader.read(0, proc);
    final long readTime_nS = System.nanoTime() - startTime_nS;
    assert proc.n == numItems;

    //Sort
    println("Sort input data");
    startTime_nS = System.nanoTime();
    Arrays.sort(dataArr);
    final long sortTime_nS = System.nanoTime() - startTime_nS;
    println("Sort done");

    //Compute CDF & PMF
    minV = dataArr[0];
    maxV = dataArr[numItems - 1];
    assert maxV > minV;

    //outputs
    //ranksArr and quantilesArr already initialized
    spArr = buildSplitPointsArr(minV, maxV, ppo);
    checkMonotonic(spArr);
    numSP = spArr.length;
    spCounts = new long[numSP];

    println("Process Percentiles");
    printPercentiles(dataArr);
    println("End Percentiles");
    println("");

    println("Process Array");
    startTime_nS = System.nanoTime();
    processArray(dataArr, spArr, spCounts, ranksArr, quantilesArr, cdfIdxArr);
    final long processTime_nS = System.nanoTime() - startTime_nS;
    println("End Process Array");
    println("");
    printCDF();
    println("");
    printPMF();
    println("");
    printTimes(readTime_nS, sortTime_nS, processTime_nS);
  }

  private void printPercentiles(final int[] sortedArr) {
    final int len = sortedArr.length;
    println("Percentiles");
    println(String.format("%6s%20s%20s", "Frac", "Index", "Value"));
    for (int i = 0; i < 100; i++) {
      final double frac = i * .01;
      final int index = (int) (frac * len);
      final int v = sortedArr[index];
      println(String.format("%6.2f%20d%20d", frac, index, v));
    }
  }

  private static void processArray(final int[] sortedArr, final double[] spArr, final long[] spCounts,
      final double[] ranksArr, final double[] quantilesArr, final int[] cdfIdxArr) {
    int rankIdx = 0;
    int spIdx = 0;
    final int dataLen = sortedArr.length;
    final int dataLenM1 = dataLen - 1;
    final int spLen = spArr.length;
    double lastV = sortedArr[0];
    for (int i = 0; i < dataLen; i++) {
      final double v = sortedArr[i];
      if (v < spArr[spIdx]) {
        spCounts[spIdx]++;
      } else {
        while (((spIdx + 1) < spLen) && (v >= spArr[spIdx])) {
          spIdx++;
        }
        spCounts[spIdx]++;
      }
      //cdf
      if (i == dataLenM1) {
        rankIdx = quantilesArr.length - 1;
        quantilesArr[rankIdx] = sortedArr[dataLenM1];
        cdfIdxArr[rankIdx] = i;
      }
      if (i >= (ranksArr[rankIdx] * dataLen)) {
        quantilesArr[rankIdx] = lastV;
        cdfIdxArr[rankIdx] = i;
        rankIdx++;
      }
      lastV = v;
    }
  }

  @Test
  public void checkProcessArr() {
    final int[] data = new int[10000];
    for (int i = 0; i < data.length; i++) { data[i] = i; }
    final double[] spArr = buildSplitPointsArr(0, 9999, 1);
    final long[] spCounts = new long[spArr.length];
    final double[] ranksArr = buildRanksArr(101);
    final int[] cdfIdxArr = new int[101];
    final double[] quantilesArr = new double[101];
    processArray(data, spArr, spCounts, ranksArr, quantilesArr, cdfIdxArr);
    System.out.println("Done");
  }

  /**
   * Compute the split-points for the PMF function.
   * The minimum split-point will never be less than one.
   * This assumes all points are positive and may include zero.
   * @param min The minimum value recorded by the sketch
   * @param max The maximum value recorded by the sketch
   * @param ppo desired number of points per Octave.
   * @return the split-points array, which does not include zero
   */
  private double[] buildSplitPointsArr(final double min, final double max, final int ppo) {
    final double ceilPwr2min = ceilingPowerOf2double(min);
    final double ceilPwr2max = ceilingPowerOf2double(max);
    final int numSP = (int)((round(log2(ceilPwr2max)) - round(log2(ceilPwr2min)) ) * ppo) + 1;
    spArr = new double[numSP];
    spArr[0] = ceilPwr2min;
    double next = ceilPwr2min;
    for (int i = 1; i < numSP; i++) {
      next = pwr2LawNextDouble(ppo, next, false);
      spArr[i] = next;
    }
    return spArr;
  }

  private static void checkMonotonic(final double[] arr) {
    final int len = arr.length;
    for (int i = 1; i < len; i++) {
      assert arr[i] > arr[i - 1];
    }
  }

  @Test
  public void checkBuildSPArr() {
    final double[] arr = buildSplitPointsArr(512, 999, 2);
    for (int i = 0; i < arr.length; i++) { System.out.println("" + arr[i]); }
  }

  /**
   * Compute the ranks array.
   * @param numRanks the number of evenly-spaced rank values including 0 and 1.0.
   * @return the ranks array
   */
  private static double[] buildRanksArr(final int numRanks) {
    final int numRanksM1 = numRanks - 1;
    final double[] fractions = new double[numRanks];
    final double delta = 1.0 / (numRanksM1);
    double d = 0.0;
    for (int i = 0; i < numRanks; i++) {
      fractions[i] = d;
      d += delta;
      d = rint(d * numRanksM1) / numRanksM1;
    }
    return fractions;
  }

  private void printCDF() {
    println("CDF");
    println(String.format(cdfHdr, "Num", "Rank", "Index", "Quantile"));
    for (int i = 0; i < numRanks; i++) {
      final String s = String.format(cdfFmt, i, ranksArr[i], cdfIdxArr[i], quantilesArr[i]);
      println(s);
    }
  }

  private void printPMF() {
    println("PMF");
    println(String.format(pmfHdr, "Index", "Quantile", "Mass"));
    int i;
    for (i = 0; i < numSP; i++) {
      println(String.format(pmfFmt, i, spArr[i], spCounts[i]));
    }
  }

  private void printTimes(final long readTime_nS, final long sortTime_nS, final long processTime_nS) {
    final double readTime_S = readTime_nS / 1E9;
    println(String.format("ReadTime_Sec  :\t%10.3f", readTime_S));
    println(String.format("ReadRate/Sec  :\t%,10.0f", proc.n / readTime_S));
    println(String.format("SortTime_nS   :\t%,10d", sortTime_nS));
    println(String.format("ProcessTime_mSec  :\t%10.3f", processTime_nS / 1E6));
    println(String.format("PT/Point_nSec:\t%10.3f", (double)processTime_nS / numRanks));
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
      println("Unzipping data file: " + srcZipFile + "...");
      UnzipFiles.unzip(srcZipFile, parent);
      if (!zipFile.exists()) {
        throw new IllegalArgumentException("Unsuccessful Unzip.");
      }
      println(srcZipFile + " unzipped!");
      dataWasZipped = true;
    }
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

  @Override
  public void println(final String s) {
    job.println(s);
  }

  // Callback
  class Process implements ProcessLine {
    int n = 0;

    @Override
    public void process(final String strArr0, final int lineNo) {
      if ((lineNo % reportInterval) == 0) {
        println("" + lineNo);
      }
      dataArr[n++] = Integer.parseInt(strArr0);
    }
  }

}
