/*
 * Copyright 2015, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import static java.lang.Math.abs;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.log10;
import static java.lang.Math.min;
import static java.lang.Math.round;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;

import com.yahoo.sketches.TestingUtil2.DoublePair;

public class RandomPowerLawGenerator {
  private Random rand = new Random();

  public static class DoublePairComparitorX implements Comparator<DoublePair> {
    @Override
    public int compare(final DoublePair p1, final DoublePair p2) {
      return Double.compare(p1.x, p2.x);
    }
  }

  // Derived
  private DoublePair logP1_;
  private DoublePair logP2_;
  private double logSlope_;

  /**
   *
   * @param p1 given
   * @param p2 given
   */
  public RandomPowerLawGenerator(final DoublePair p1, final DoublePair p2) {
    checkPairs(p1, p2);
    rand = new Random();
    logP1_ = logPair(p1);
    logP2_ = logPair(p2);
    logSlope_ = slope(logP1_, logP2_);
  }

  /**
   * Get random Pair in domain and range of LogP1, LogP2 and logSlope.
   *
   * @return random Pair
   */
  public DoublePair getRandomPair() {
    final double r = rand.nextDouble();
    final double logX = scale(r, logP1_.x, logP2_.x); // scale on log axis
    final double logY = pointSlopeY(logX, logP1_, logSlope_); // derive logY
    return expPair(logX, logY);
  }

  /**
   *
   * @param p1 given
   * @param p2 given
   * @param n  given
   * @return Array List
   */
  public static ArrayList<DoublePair> getRandomPowerLawPairs(final DoublePair p1,
      final DoublePair p2, final int n) {
    final Random rand = new Random();
    final DoublePair logP1 = logPair(p1);
    final DoublePair logP2 = logPair(p2);
    final double logSlope = slope(logP1, logP2);
    final ArrayList<DoublePair> pairArr = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      final double randX = rand.nextDouble();
      final DoublePair out = getPairOnLine(randX, logP1, logP2, logSlope);
      out.x = exp(out.x);
      out.y = exp(out.y);
      pairArr.add(out);
    }
    return pairArr;
  }

  /**
   * Returns a pair on the line between p1 and p2 based on the input value x,
   * which must be between 0 and 1, and the slope.
   *
   * @param x the normalized input X-axis
   * @param p1 point 1
   * @param p2 point 2
   * @param slope the given slope
   * @return a pair on the line between p1 and p2
   */
  public static DoublePair getPairOnLine(final double x, final DoublePair p1, final DoublePair p2,
      final double slope) {
    final double xOut = scale(x, p1.x, p2.x);
    final double yOut = pointSlopeY(xOut, p1, slope);
    return new DoublePair(xOut, yOut);
  }

  /**
   * Scales a value v in the range 0 to 1 into the range min to max.
   *
   * @param v the given value of x in the range 0 to 1.
   * @param min the minimum endpoint of the resulting range
   * @param max the maximum endpoint of the resulting range
   * @return scaled x
   */
  public static double scale(final double v, final double min, final double max) {
    final double delta = abs(min - max);
    final double actMin = min(min, max);
    return (v * delta) + actMin;
  }

  public static DoublePair logPair(final DoublePair p) {
    return new DoublePair(log(p.x), log(p.y));
  }

  public static DoublePair expPair(final double x, final double y) {
    return new DoublePair(exp(x), exp(y));
  }

  public static DoublePair expPair(final DoublePair p) {
    return new DoublePair(exp(p.x), exp(p.y));
  }

  public static double slope(final DoublePair p1, final DoublePair p2) {
    return (p2.y - p1.y) / (p2.x - p1.x);
  }

  public static double twoPointY(final double x, final DoublePair p1, final DoublePair p2) {
    final double slope = slope(p1, p2);
    return pointSlopeY(x, p1, slope);
  }

  public static double pointSlopeY(final double x, final DoublePair p, final double slope) {
    return (slope * (x - p.x)) + p.y;
  }

  public static double pointSlopeY(final double x, final double slope, final double x0,
      final double y0) {
    return (slope * (x - x0)) + y0;
  }

  private static void checkPairs(final DoublePair p1, final DoublePair p2) {
    if ((p1.x == p2.x) || (p1.y == p2.y)) {
      throw new IllegalArgumentException("X or Y values cannot be equal");
    }
  }

  /**
   * @param args given
   */
  public static void main(final String[] args) {
    final String fmt = "%,20.2f" + "%,20.2f";
    final String hfmt = "%20s"   + "%20s";
    println(String.format(hfmt, "X", "Y"));
    final DoublePair p1 = new DoublePair(1, 100);
    final DoublePair p2 = new DoublePair(100, 1);
    final int denOOMX = 10;
    final RandomPowerLawGenerator rpl = new RandomPowerLawGenerator(p1, p2);
    final double oomx = abs(log10(p1.x) - log10(p2.x));
    final int n = (int) round(oomx * denOOMX);
    for (int i = 0; i < n; i++) {
      final DoublePair p = rpl.getRandomPair();
      println(String.format(fmt, p.x, p.y));
    }
  }

  /**
   * @param s value to print
   */
  static void println(final String s) {
    System.out.println(s); // disable here
  }

}
