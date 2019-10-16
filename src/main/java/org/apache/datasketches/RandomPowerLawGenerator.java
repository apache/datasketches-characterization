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

package org.apache.datasketches;

import static java.lang.Math.abs;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.log10;
import static java.lang.Math.min;
import static java.lang.Math.round;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;

public class RandomPowerLawGenerator {
  private static Random rand = new Random();

  // Derived
  private DoublePair logP1_;
  private DoublePair logP2_;
  private double logSlope_;

  /**
   * Constructs this generator with two given end-points.
   * The x, y values must be positive and greater than zero.
   * @param p1 one end of the line.
   * @param p2 the other end of the line
   */
  public RandomPowerLawGenerator(final DoublePair p1, final DoublePair p2) {
    checkPairs(p1, p2);
    logP1_ = logPair(p1);
    logP2_ = logPair(p2);
    logSlope_ = slope(logP1_, logP2_);
  }

  /**
   * Get random Pair on the line in the log-log plane between the two given points.
   * @return random Pair on the line in the log-log plane between the two given points.
   */
  public DoublePair getRandomPowerLawPair() {
    final double r = rand.nextDouble();
    final double logX = scale(r, logP1_.x, logP2_.x); // scale on log axis
    final double logY = pointSlopeY(logX, logP1_, logSlope_); // derive logY
    return expPair(logX, logY);
  }

  /**
   * A random Pair on the line in the log-log plane between the two given points.
   * The x, y values for the pairs must be positive and greater than zero.
   * @param p1 one end of the line.
   * @param p2 the other end of the line
   * @return a Pair on the line in the log-log plane between the two given points.
   */
  public static DoublePair getRandomPowerLawPair(final DoublePair p1, final DoublePair p2) {
    final DoublePair logP1 = logPair(p1);
    final DoublePair logP2 = logPair(p2);
    final double logSlope = slope(logP1, logP2);
    final double randX = rand.nextDouble();
    final DoublePair out = getPairOnLine(randX, logP1, logP2, logSlope);
    return out;
  }

  /**
   * An Array List of n points on the power-law line in the log-log plane between the two given
   * points. The x, y values for the pairs must be positive and greater than zero.
   * @param p1 one end of the line
   * @param p2 the other end of the line
   * @param n the number of points
   * @return Array List of n points on the power-law line in the log-log plane between the two
   * given points.
   */
  public static ArrayList<DoublePair> getRandomPowerLawPairs(final DoublePair p1,
      final DoublePair p2, final int n) {
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
   * Returns a pair on the line between p1 and p2 based on the normalized input value x,
   * which must be between 0 and 1, and the slope. This is linear scaling for a linear plot.
   *
   * @param x the normalized input X-axis
   * @param p1 point 1
   * @param p2 point 2
   * @param slope the given slope. Usually computed once for a number of points. Passed in for speed.
   * @return a pair on the line between p1 and p2
   */
  public static DoublePair getPairOnLine(final double x, final DoublePair p1, final DoublePair p2,
      final double slope) {
    final double xOut = scale(x, p1.x, p2.x);
    final double yOut = pointSlopeY(xOut, p1, slope);
    return new DoublePair(xOut, yOut);
  }

  /**
   * Linearly scales the given value v in the range 0 to 1 into the range min to max.
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

  /**
   * Returns a DoublePair containing log(x) and log(y) from the given DoublePair.
   * @param p the given DoublePair
   * @return a DoublePair containing log(x) and log(y) from the given DoublePair.
   */
  public static DoublePair logPair(final DoublePair p) {
    return new DoublePair(log(p.x), log(p.y));
  }

  /**
   * Returns a DoublePair containing exp(x) and exp(y) from the given x and y.
   * @param x the given x-coordinate
   * @param y the given y-coordinate
   * @return a DoublePair containing exp(x) and exp(y) from the given x and y.
   */
  public static DoublePair expPair(final double x, final double y) {
    return new DoublePair(exp(x), exp(y));
  }

  /**
   * Returns a DoublePair containing exp(x) and exp(y) from the given DoublePair.
   * @param p the given DoublePair
   * @return a DoublePair containing exp(x) and exp(y) from the given DoublePair.
   */
  public static DoublePair expPair(final DoublePair p) {
    return new DoublePair(exp(p.x), exp(p.y));
  }

  /**
   * Returns the slope of the line between p1 and p2. This assumes linear x and y.
   * @param p1 the first point
   * @param p2 the second point
   * @return the slope of the line between p1 and p2. This assumes linear x and y.
   */
  public static double slope(final DoublePair p1, final DoublePair p2) {
    return (p2.y - p1.y) / (p2.x - p1.x);
  }

  /**
   * Given two points, p1 and p2, and a value x, this returns the value y on the line that
   * intersects p1 and p2.
   * @param x the given x
   * @param p1 the first point
   * @param p2 the secpmd point
   * @return the value y on the line that intersects p1 and p2, given x.
   */
  public static double twoPointY(final double x, final DoublePair p1, final DoublePair p2) {
    final double slope = slope(p1, p2);
    return pointSlopeY(x, p1, slope);
  }

  /**
   * Given a point p, a slope, and a value x, this returns the value y on the line that
   * intersects the point p with the given slope.
   * @param x the given x
   * @param p the given point
   * @param slope the given slope
   * @return the value y on the line that intersects the point p with the given slope and x.
   */
  public static double pointSlopeY(final double x, final DoublePair p, final double slope) {
    return (slope * (x - p.x)) + p.y;
  }

  /**
   * Given a point (x0, y0), a slope, and a value x, this returns the value y on the line that
   * intersects the point p with the given slope.
   * @param x the given x
   * @param x0 the x-coordinate of the given point
   * @param y0 the y-coordinate of the given point
   * @param slope the given slope
   * @return this returns the value y on the line that intersects the point x0, y0 with the
   * given slope.
   */
  public static double pointSlopeY(final double x, final double x0, final double y0,
      final double slope) {
    return (slope * (x - x0)) + y0;
  }

  public static class DoublePairComparitorX implements Comparator<DoublePair> {
    @Override
    public int compare(final DoublePair p1, final DoublePair p2) {
      return Double.compare(p1.x, p2.x);
    }
  }

  /**
   * @param s value to print
   */
  public static void println(final String s) {
    System.out.println(s); // disable here
  }

  /**
   * @param args not used
   */
  public static void main(final String[] args) {
    final String fmt = "%,20.2f" + "%,20.2f";
    final String hfmt = "%20s"   + "%20s";
    println(String.format(hfmt, "X", "Y"));
    final DoublePair p1 = new DoublePair(1, 100);
    final DoublePair p2 = new DoublePair(100, 1);

    final RandomPowerLawGenerator rpl = new RandomPowerLawGenerator(p1, p2);

    final double oomx = abs(log10(p1.x) - log10(p2.x)); //the OOM of the input X
    final int denOOMX = 10; //desired avg # of points per OOM on X
    final int n = (int) round(oomx * denOOMX);   //compute this number of points
    for (int i = 0; i < n; i++) {
      final DoublePair p = rpl.getRandomPowerLawPair();
      println(String.format(fmt, p.x, p.y));
    }
  }

  private static void checkPairs(final DoublePair p1, final DoublePair p2) {
    if ((p1.x == p2.x) || (p1.y == p2.y)) {
      throw new IllegalArgumentException("X or Y values cannot be equal");
    }
  }

}
