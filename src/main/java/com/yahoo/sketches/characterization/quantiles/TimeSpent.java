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
  String srcFileName;
  LineReader lineReader;
  UpdateDoublesSketch sketch;
  int reportInterval; //prints number of lines read to console every reportInterval lines.
  int numRanks; //number of linearly spaced ranks between zero and one.
  int ppOoM; //number of split-points per Order-Of-Magnitude (OOM).
  int k; //size of sketch
  long n; //number of non-empty lines read
  long startTime;
  long endReadTime;
  long endAnalTime;

  // Callback
  class Process implements ProcessLine {

    @Override
    public void process(final String strArr0, final int lineNo) {
      if ((lineNo % reportInterval) == 0) {
        println("" + lineNo);
      }
      final long v = Long.parseLong(strArr0);
      if (v == 0) {
        return;
      }
      sketch.update(v);
    }
  }

  // Constructor
  /**
   * blah
   * @param srcFileName the input file name as a full path plus name.
   * @param reportInterval prints number of lines read to console every reportInterval lines.
   * @param numRanks number of linearly spaced ranks between zero and one.
   * @param ppOoM number of exponential split-points per Order-Of-Magnitude (OOM).
   * @param k the size of the sketch.
   */
  public TimeSpent(final String srcFileName, final int reportInterval, final int numRanks,
      final int ppOoM, final int k) {
    this.srcFileName = srcFileName;
    lineReader = new LineReader(srcFileName);
    this.reportInterval = reportInterval;
    this.numRanks = numRanks;
    this.ppOoM = ppOoM;
    this.k = k;
    sketch = DoublesSketch.builder().setK(k).build();
  }

  /**
   * Reads the input file as strings separted by line separator.
   */
  public void readFile() {
    startTime = System.currentTimeMillis();
    lineReader.read(0, new Process());
    endReadTime = System.currentTimeMillis();
  }

  /**
   * Print CDF of ranks to quantiles.
   * Print PMF
   */
  public void analysis() {
    final double[] fracRanks = buildRanksArr(numRanks);
    final double[] quantiles = sketch.getQuantiles(fracRanks);
    final double minV = sketch.getMinValue();

    //print sketch stats
    final double maxV = sketch.getMaxValue();
    println("\tMax:\t" + maxV);
    n = sketch.getN();
    println("N:\t" + n);
    println("Size Bytes:\t" + sketch.getCompactStorageBytes());
    println(sketch.toString());
    println("");

    //print the cumulative rank to quantiles distribution
    println("\tMin:\t" + minV);
    for (int i = 0; i < numRanks; i++) {
      println(i + "\t" + fracRanks[i] + "\t" + quantiles[i]);
    }
    println("");

    //print PMF histogram, assume 5 points per Order-Of-Magnitude (OOM).
    final double[] splitpoints = buildSplitPointsArr(minV, maxV, 5);
    final double[] pmfArr = sketch.getPMF(splitpoints);
    final int lenPMF = pmfArr.length;
    int i;
    for (i = 0; i < (lenPMF - 1); i++) {
      println(i + "\t" + splitpoints[i] + "\t" + (pmfArr[i] * n));
    }
    println(i + "\t\t" + (pmfArr[i] * n)); // the last point

    final double readTime = (endReadTime - startTime) / 1000.0;
    final double analTime = (System.currentTimeMillis() - endReadTime) / 1000.0;
    println("ReadTime:\t" + readTime);
    println("ReadRate:\t" + (n / readTime));
    println("AnalTime:\t" + analTime);

  }

  /**
   * Compute the split-points for the PMF function.
   * @param min The minimum value recorded by the sketch
   * @param max The maximum value recorded by the sketch
   * @param ppmag desired number of points per Order-Of-Magnitude (OOM).
   * @return the split-points array
   */
  public static double[] buildSplitPointsArr(final double min, final double max, final int ppmag) {
    final double log10Min = floor(log10(min));
    final double log10Max = ceil(log10(max));
    final int oom = (int) (log10Max - log10Min);
    final int points = (oom * ppmag) + 1;
    final double[] ptsArr = new double[points];
    double sp = log10Min;
    final double delta = 1.0 / ppmag;
    for (int i = 0; i < points; i++) {
      ptsArr[i] = pow(10, sp);
      sp += delta;
      sp = rint(sp * ppmag) / ppmag;
    }
    return ptsArr;
  }

  /**
   * Compute the ranks array.
   * @param numRanks the number of evenly-spaced rank values including 0 and 1.0.
   * @return the ranks array
   */
  public static double[] buildRanksArr(final int numRanks) {
    final int numRM1 = numRanks - 1;
    final double[] fractions = new double[numRanks];
    final double delta = 1.0 / (numRM1);
    double d = 0.0;
    for (int i = 0; i < numRanks; i++) {
      fractions[i] = d;
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

    final String fileName =
      "/Users/lrhodes/dev/git/characterization/src/main/resources/quantiles/streamA.txt";
    final int reportInterval = 10_000_000;
    final int numRanks = 101;
    final int ppOoM = 5;
    final int k = 256;

    final TimeSpent ts = new TimeSpent(fileName, reportInterval, numRanks, ppOoM, k);

    ts.readFile();
    println("");
    ts.analysis();
  }

}
