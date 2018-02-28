/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import static java.lang.Math.abs;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.min;

import java.util.Comparator;
import java.util.Random;

public class TestingUtil2 {
  public static Random rand = new Random();

  public static class LongPair implements Comparator<LongPair> {
    public long x;
    public long y;

    public LongPair(final long x, final long y) {
      this.x = x;
      this.y = y;
    }

    @Override
    public int compare(final LongPair p1, final LongPair p2) {
      return (p1.x < p2.x) ? -1 : (p1.x > p2.x) ? 1 : 0;
    }
  }

  public static class DoublePair implements Comparator<DoublePair> {
    public double x;
    public double y;

    public DoublePair(final double x, final double y) {
      this.x = x;
      this.y = y;
    }

    @Override
    public int compare(final DoublePair p1, final DoublePair p2) {
      return Double.compare(p1.x, p2.x); // handles NaN, +/- 0, etc.
    }
  }

  /**
   *
   * @param x1 given
   * @param x2 given
   * @param points given
   * @param log given
   * @return double array
   */
  public static double[] evenlySpaced(final double x1, final double x2, final int points,
      final boolean log) {
    if (points <= 0) {
      throw new IllegalArgumentException("points must be > 0");
    }
    final double[] out = new double[points];
    out[0] = x1;
    if (points == 1) {
      return out;
    }
    if (points == 2) {
      out[1] = x2;
      return out;
    }
    // 3 or more
    if (log) {
      if ((x2 <= 0) || (x1 <= 0)) {
        throw new IllegalArgumentException("x1 and x2 must be > 0.");
      }
      final double logMin = log(x1);
      final double delta = log(x2 / x1) / (points - 1);
      for (int i = 1; i < points; i++) {
        out[i] = exp(mXplusY(delta, i, logMin));
      }
    } else { // linear
      final double delta = (x2 - x1) / (points - 1);
      for (int i = 1; i < points; i++) {
        out[i] = mXplusY(delta, i, x1);
      }
    }
    return out;
  }

  /**
   *
   * @param x1 given
   * @param x2 given
   * @param points given
   * @param log given
   * @return double array
   */
  public static double[] uniformRandom(final double x1, final double x2, final int points,
      final boolean log) {
    if (points <= 0) {
      throw new IllegalArgumentException("points must be > 0");
    }
    final double[] out = new double[points];

    if (log) {
      if ((x2 <= 0) || (x1 <= 0)) {
        throw new IllegalArgumentException("x1 and x2 must be > 0.");
      }
      final double logX1 = log(x1);
      final double delta = log(x2) - logX1;
      for (int i = 0; i < points; i++) {
        out[i] = exp(mXplusY(delta, rand.nextDouble(), logX1));
      }
    } else {
      final double delta = x2 - x1;
      for (int i = 0; i < points; i++) {
        out[i] = mXplusY(delta, rand.nextDouble(), x1);
      }
    }
    return out;
  }

  /**
   * Scales a value v in the range 0 to 1 into the range min to max.
   *
   * @param v
   *          the given value of x in the range 0 to 1.
   * @param min
   *          the minimum endpoint of the resulting range
   * @param max
   *          the maximum endpoint of the resulting range
   * @return scaled x
   */
  public static double scale(final double v, final double min, final double max) {
    final double delta = abs(min - max);
    final double actMin = min(min, max);
    return mXplusY(delta, v, actMin);
  }

  public static final double linXlinYline(final double slope, final double x, final double x0,
      final double y0) {
    return mXplusY(slope, (x - x0), y0);
  }

  public static final double logXlogYline(final double slope, final double x, final double x0,
      final double y0) {
    return exp(mXplusY(slope, log(x / x0), log(y0)));
  }

  public static final double mXplusY(final double m, final double x, final double y) {
    return (m * x) + y;
  }

}
