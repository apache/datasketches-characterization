/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.quantiles;

import static java.lang.Math.ceil;
import static java.lang.Math.floor;
import static java.lang.Math.log10;
import static java.lang.Math.pow;
import static java.lang.Math.rint;

import java.io.File;

import com.yahoo.sketches.LineReader;
import com.yahoo.sketches.ProcessLine;
import com.yahoo.sketches.UnzipFiles;
import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.characterization.Properties;
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
  private int ppOoM; //number of split-points per Order-Of-Magnitude (OOM).
  private int histSize; //ApproximateHistogram size. Max # of position, bin pairs
  private String cdfHdr;
  private String cdfFmt;
  private String pmfHdr;
  private String pmfFmt;

  ApproximateHistogram ahist;
  Histogram hist;
  private boolean dataWasZipped = false;

  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
    final Properties prop = job.getProperties();
    //Get Properties
    srcFileName = prop.mustGet("FileName");
    reportInterval = Integer.parseInt(prop.mustGet("ReportInterval"));
    numRanks = Integer.parseInt(prop.mustGet("NumRanks"));
    ppOoM = Integer.parseInt(prop.mustGet("PpOoM"));

    cdfHdr = prop.mustGet("CdfHdr").replace("\\t", "\t");
    cdfFmt = prop.mustGet("CdfFmt").replace("\\t", "\t");
    pmfHdr = prop.mustGet("PdfHdr").replace("\\t", "\t");
    pmfFmt = prop.mustGet("PdfFmt").replace("\\t", "\t");

    histSize = Integer.parseInt(prop.mustGet("HistSize"));
    ahist = new ApproximateHistogram(histSize);

    processStream();

    if (dataWasZipped) {
      final File file = new File(srcFileName);
      if (file.exists()) { file.delete(); }
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

    @Override
    public void process(final String strArr0, final int lineNo) {
      if ((lineNo % reportInterval) == 0) {
        println("" + lineNo);
      }
      final long v = Long.parseLong(strArr0);
      ahist.offer(v);
    }
  }

  /**
   * Read file, Print CDF, Print PMF.
   */
  private void processStream() {
    checkIfZipped(srcFileName);

    //Read
    //println("");
    println("Input Lines Processed: ");
    final LineReader lineReader = new LineReader(srcFileName);

    final long startReadTime_nS = System.nanoTime();
    lineReader.read(0, new Process());
    final long readTime_nS = System.nanoTime() - startReadTime_nS;

    //print hist stats
    println(ahist.toString());

    //CDF
    final float[] fracRanks = buildRanksArr(numRanks);
    final long startCdfTime_nS = System.nanoTime();
    final float[] quantiles = ahist.getQuantiles(fracRanks);
    final long cdfTime_nS = System.nanoTime() - startCdfTime_nS;

    //print the cumulative rank to quantiles distribution
    println("");
    println("CDF");
    println(String.format(cdfHdr, "Index", "Rank", "Quantile"));
    for (int i = 0; i < numRanks; i++) {
      final String s = String.format(cdfFmt, i, fracRanks[i], quantiles[i]);
      println(s);
    }
    println("");

    //print PMF histogram, using Points Per Order-Of-Magnitude (ppOoM).
    final double minV = ahist.getMin();
    final double maxV = ahist.getMax();
    final double n = ahist.count();
    final float[] splitpoints = buildExpBreaksArr(minV, maxV, ppOoM);
    //PMF
    final long startPmfTime_nS = System.nanoTime();
    final Histogram hist = ahist.toHistogram(splitpoints);

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
    println(String.format("ReadRate/Sec  :\t%,10.0f", n / readTime_S));
    println(String.format("CdfTime_mSec  :\t%10.3f", cdfTime_nS / 1E6));
    println(String.format("Cdf/Point_nSec:\t%10.3f", (double)cdfTime_nS / numRanks));
    println(String.format("PmfTime_mSec  :\t%10.3f", pmfTime_nS / 1E6));
    println(String.format("Pmf/Point_nSec:\t%10.3f", (double)pmfTime_nS / lenBreaks));
  }

  /**
   * Compute the split-points for the PMF function.
   * @param min The minimum value recorded by the sketch
   * @param max The maximum value recorded by the sketch
   * @param ppmag desired number of points per Order-Of-Magnitude (OOM).
   * @return the split-points array
   */
  private static float[] buildExpBreaksArr(final double min, final double max, final int ppmag) {
    final boolean minLT1 = min < 1.0;
    final double startLgMin = minLT1 ? 1.0 : min;
    final double log10Min = floor(log10(startLgMin));
    final double log10Max = ceil(log10(max));
    final int oom = (int) (log10Max - log10Min);
    final int points = (oom * ppmag) + 1 + (minLT1 ? 1 : 0);
    final float[] ptsArr = new float[points];
    double sp = log10Min;
    final double delta = 1.0 / ppmag;
    for (int i = 0; i < points; i++) {
      if (minLT1 && (i == 0)) { ptsArr[i] = 0; continue; }
      ptsArr[i] = (float) pow(10, sp);
      sp += delta;
      sp = rint(sp * ppmag) / ppmag;
    }
    return ptsArr;
  }

  /**
   * Compute the ranks array.
   * @param numRanks the number of evenly-spaced rank values excluding 0 and 1.0.
   * @return the ranks array
   */
  private static float[] buildRanksArr(final int numRanks) {
    final int numRM1 = numRanks + 1;
    final float[] fractions = new float[numRanks];
    final double delta = 1.0 / (numRM1);
    double d = delta;
    for (int i = 0; i < numRanks; i++) {
      fractions[i] = (float) d;
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
      println("Unzipping data file: " + srcZipFile + "...");
      UnzipFiles.unzip(srcZipFile, parent);
      if (!zipFile.exists()) {
        throw new IllegalArgumentException("Unsuccessful Unzip.");
      }
      println(srcZipFile + " unzipped!");
      dataWasZipped = true;
    }
  }

}
