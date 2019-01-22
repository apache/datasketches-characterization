/*
 * Copyright 2015, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.quantiles;

import static java.lang.Math.ceil;
import static java.lang.Math.floor;
import static java.lang.Math.log10;
import static java.lang.Math.pow;
import static java.lang.Math.rint;

import com.yahoo.sketches.LineReader;
import com.yahoo.sketches.ProcessLine;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

public class TimeSpent {
  String fileName_;
  LineReader reader_;
  UpdateDoublesSketch qs_;
  int reportInterval_ = 10000000;
  int numRanks_;
  int k_;
  long n_;
  long startTime_;
  long endReadTime_;
  long endAnalTime_;

  // Callback
  class Process implements ProcessLine {

    @Override
    public void process(final String strArr0, final int lineNo) {
      if ((lineNo % reportInterval_) == 0) {
        println("" + lineNo);
      }
      final long v = Long.parseLong(strArr0);
      if (v == 0) {
        return;
      }
      qs_.update(v);
    }
  }

  // Constructor
  /**
   * blah
   * @param fileName blah
   * @param reportInterval blah
   * @param numRanks blah
   * @param k blah
   */
  public TimeSpent(final String fileName, final int reportInterval, final int numRanks, final int k) {
    fileName_ = fileName;
    reader_ = new LineReader(fileName);
    reportInterval_ = reportInterval;
    numRanks_ = numRanks;
    k_ = k;
    qs_ = DoublesSketch.builder().setK(k).build();
  }

  /**
   * blah
   * @param lines blah
   */
  public void readFile(final int lines) {
    startTime_ = System.currentTimeMillis();
    reader_.read(lines, new Process());
    endReadTime_ = System.currentTimeMillis();
  }

  /**
   * blah
   */
  public void analysis() {
    final double[] frac = buildRanksArr();
    final double[] values = qs_.getQuantiles(frac);
    final double minV = qs_.getMinValue();

    println("\tMin:\t" + minV);
    for (int i = 0; i < numRanks_; i++) {
      println(i + "\t" + frac[i] + "\t" + values[i]);
    }
    final double maxV = qs_.getMaxValue();
    println("\tMax:\t" + maxV);
    n_ = qs_.getN();
    println("N:\t" + n_);
    println("Size Bytes:\t" + qs_.getCompactStorageBytes());
    println("");
    final double[] splitpoints = buildSplitPointsArr(minV, maxV, 5);
    final double[] pmfArr = qs_.getPMF(splitpoints);
    final int lenPMF = pmfArr.length;
    int i;
    for (i = 0; i < (lenPMF - 1); i++) {
      println(i + "\t" + splitpoints[i] + "\t" + (pmfArr[i] * n_));
    }
    println(i + "\t\t" + (pmfArr[i] * n_)); // the last point

    final double readTime = (endReadTime_ - startTime_) / 1000.0;
    final double analTime = (System.currentTimeMillis() - endReadTime_) / 1000.0;
    println("ReadTime:\t" + readTime);
    println("ReadRate:\t" + (n_ / readTime));
    println("AnalTime:\t" + analTime);

  }

  /**
   * blah
   * @param min blah
   * @param max blah
   * @param ppmag v
   * @return blah
   */
  public double[] buildSplitPointsArr(final double min, final double max, final int ppmag) {
    final double log10Min = floor(log10(min));
    final double log10Max = ceil(log10(max));
    final int oom = (int) (log10Max - log10Min);
    final int points = (oom * ppmag) + 1;
    final double[] ptsArr = new double[points];
    double sp = log10Min;
    final double delta = 1.0 / ppmag;
    for (int i = 0; i < points; i++) {
      ptsArr[i] = pow(10, sp);
      // println(""+ptsArr[i]);
      sp += delta;
      sp = rint(sp * ppmag) / ppmag;
    }
    return ptsArr;
  }

  /**
   * blah
   * @return blah
   */
  public double[] buildRanksArr() {
    final int numRM1 = numRanks_ - 1;
    final double[] fractions = new double[numRanks_];
    final double delta = 1.0 / (numRM1);
    double d = 0.0;
    for (int i = 0; i < numRanks_; i++) {
      fractions[i] = d;
      // println(""+d);
      d += delta;
      d = rint(d * numRM1) / numRM1;
    }
    return fractions;
  }

  static void println(final String s) {
    System.out.println(s);
  }

  /**
   * blah
   * @param args blah
   */
  public static void main(final String[] args) {
    final int reportInterval = 10000000;
    final String fileName = "/Users/lrhodes/dev/Data/timespent_263Mvalues_20160303.txt";
    final int numRanks = 101;
    final int k = 256;
    final int numValues = 0;

    final TimeSpent ts = new TimeSpent(fileName, reportInterval, numRanks, k);
    // ts.buildRanksArr();
    // ts.buildSplitPointsArr(1.0, 1E6, 5);

    ts.readFile(numValues);
    println("");
    ts.analysis();
  }

}
