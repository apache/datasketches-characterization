/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.quantiles;

import static com.yahoo.sketches.characterization.quantiles.ProfileUtil.buildSplitPointsArr;

import java.io.File;

import org.testng.annotations.Test;

import com.yahoo.sketches.Job;
import com.yahoo.sketches.JobProfile;
import com.yahoo.sketches.LineReader;
import com.yahoo.sketches.ProcessLine;
import com.yahoo.sketches.Properties;
import com.yahoo.sketches.UnzipFiles;
import com.yahoo.sketches.characterization.quantiles.druidhistogram.ApproximateHistogram;
import com.yahoo.sketches.characterization.quantiles.druidhistogram.Histogram;

/**
 * @author Lee Rhodes
 */
public class DruidAppHistStreamAProfile implements JobProfile {

  private Job job;

  //Properties
  private String srcFileName;
  private int reportInterval; //prints number of lines read to console every reportInterval lines.
  private int numRanks; //number of linearly spaced ranks between zero and one.
  private int pplb; //number of split-Points Per Log Base.
  private double logBase; //Log Base
  private int histSize; //ApproximateHistogram size. Max # of position, bin pairs
  private String cdfHdr;
  private String cdfFmt;
  private String pmfHdr;
  private String pmfFmt;

  ApproximateHistogram ahist;
  Histogram hist;

  private boolean dataWasZipped = false;
  private double eps = 1e-6;
  private Process proc = new Process();

  //outputs for plotting
  private double minV;
  private double maxV;

  private float[] spArr;  //splitpoints 0, 1 ... ceilPwr2(maxValue), ppo values per octave
  private int numSP;
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
    logBase = Double.parseDouble(prop.mustGet("LogBase"));
    pplb = Integer.parseInt(prop.mustGet("PPLB"));

    histSize = Integer.parseInt(prop.mustGet("HistSize"));

    cdfHdr = prop.mustGet("CdfHdr").replace("\\t", "\t");
    cdfFmt = prop.mustGet("CdfFmt").replace("\\t", "\t");
    pmfHdr = prop.mustGet("PdfHdr").replace("\\t", "\t");
    pmfFmt = prop.mustGet("PdfFmt").replace("\\t", "\t");

    ahist = new ApproximateHistogram(histSize);

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
    println("Input Lines Processed: ");
    final LineReader lineReader = new LineReader(srcFileName);

    final long startReadTime_nS = System.nanoTime();
    lineReader.read(0, proc);
    final long readTime_nS = System.nanoTime() - startReadTime_nS;

    //print hist stats
    println(ahist.toString().replace(", ", "\n").replace("*", ""));
    println("Max Storage Size: " + ahist.getMaxStorageSize());

    //CDF
    final float[] fracRanks = buildRanksArr(numRanks);
    final long startCdfTime_nS = System.nanoTime();
    final float[] quantiles = ahist.getQuantiles(fracRanks);
    final long cdfTime_nS = System.nanoTime() - startCdfTime_nS;

    println("");
    println("CDF");
    println(String.format(cdfHdr, "Index", "Rank", "Quantile"));
    for (int i = 0; i < numRanks; i++) {
      final String s = String.format(cdfFmt, i, fracRanks[i], (int)quantiles[i]);
      println(s);
    }
    println("");

    //print PMF histogram, using Points Per Log Base.
    minV = ahist.getMin();
    maxV = ahist.getMax();
    numItems = ahist.count();
    final double[] splitpoints = buildSplitPointsArr(minV, maxV, pplb, logBase, eps);
    numSP = splitpoints.length;
    spArr = new float[numSP];
    for (int i = 0; i < numSP; i++) { spArr[i] = (float) splitpoints[i]; }

    //PMF
    final long startPmfTime_nS = System.nanoTime();
    final Histogram hist = ahist.toHistogram(spArr);

    final double[] breaksArr = hist.getBreaks();
    final double[] countsArr = hist.getCounts();
    final long pmfTime_nS = System.nanoTime() - startPmfTime_nS;
    final int lenBreaks = breaksArr.length;

    println("PMF");
    println(String.format(pmfHdr, "Index", "Quantile", "Mass"));
    int i;
    for (i = 0; i < (lenBreaks - 1); i++) {
      println(String.format(pmfFmt, i, breaksArr[i], countsArr[i]));
    }
    println(String.format(pmfFmt, i, breaksArr[i], 0.0)); // the last point

    final double readTime_S = readTime_nS / 1E9;
    println("");
    println(String.format("ReadTime_Sec  :\t%10.3f", readTime_S));
    println(String.format("ReadRate/Sec  :\t%,10.0f", numItems / readTime_S));
    println(String.format("CdfTime_mSec  :\t%10.3f", cdfTime_nS / 1E6));
    println(String.format("Cdf/Point_nSec:\t%10.3f", (double)cdfTime_nS / numRanks));
    println(String.format("PmfTime_mSec  :\t%10.3f", pmfTime_nS / 1E6));
    println(String.format("Pmf/Point_nSec:\t%10.3f", (double)pmfTime_nS / lenBreaks));
  }

  /**
   * Compute the ranks array.
   * @param numRanks the number of evenly-spaced rank values excluding 0 and 1.0.
   * @return the ranks array
   */
  private static float[] buildRanksArr(final int numRanks) {
    final float[] fractions = new float[numRanks];
    final double delta = 1.0 / (numRanks + 1);
    for (int i = 1; i <= numRanks; i++) {
      fractions[i - 1] = (float) (delta * i);
    }
    return fractions;
  }

  @Test
  public void checkRanks() {
    final int num = 3;
    final float[] arr = buildRanksArr(num);
    for (int i = 0; i < num; i++) { System.out.println(arr[i]); }
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
      final long v = Long.parseLong(strArr0);
      ahist.offer(v);
      n++;
    }
  }

}
