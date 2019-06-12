/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

// Based on the following implementation:
// https://github.com/apache/commons-statistics/blob/master/commons-statistics-distribution/src/main/java/org/apache/commons/statistics/distribution/ZipfDistribution.java
// https://github.com/apache/commons-rng/blob/master/commons-rng-sampling/src/main/java/org/apache/commons/rng/sampling/distribution/RejectionInversionZipfSampler.java

package com.yahoo.sketches.characterization;

import java.util.Random;

public class ZipfDistribution {

  private static final double TAYLOR_THRESHOLD = 1e-8;
  private static final double F_1_2 = 0.5;
  private static final double F_1_3 = 1d / 3;
  private static final double F_1_4 = 0.25;

  private static final Random rng = new Random();

  private int numberOfElements;
  private double exponent;

  private final double hIntegralX1;
  private final double hIntegralNumberOfElements;
  private final double s;

  /**
   *
   * @param numberOfElements blah
   * @param exponent blah
   */
  public ZipfDistribution(final int numberOfElements, final double exponent) {
    this.numberOfElements = numberOfElements;
    this.exponent = exponent;
    hIntegralX1 = hIntegral(1.5) - 1.0;
    hIntegralNumberOfElements = hIntegral(numberOfElements + F_1_2);
    s = 2 - hIntegralInverse(hIntegral(2.5) - h(2.0));
  }

  /**
   *
   * @return number of elements
   */
  public int sample() {
    while (true) {
      final double u = hIntegralNumberOfElements
          + (rng.nextDouble() * (hIntegralX1 - hIntegralNumberOfElements));

      final double x = hIntegralInverse(u);
      int k = (int) (x + F_1_2);

      if (k < 1) {
        k = 1;
      } else if (k > numberOfElements) {
        k = numberOfElements;
      }

      if (((k - x) <= s) || (u >= (hIntegral(k + F_1_2) - h(k)))) {
          return k;
      }
    }
  }

  private double hIntegral(final double x) {
    final double logX = Math.log(x);
    return helper2((1 - exponent) * logX) * logX;
  }

  private double h(final double x) {
    return Math.exp(-exponent * Math.log(x));
  }

  private double hIntegralInverse(final double x) {
    double t = x * (1 - exponent);
    if (t < -1) {
      // Limit value to the range [-1, +inf).
      // t could be smaller than -1 in some rare cases due to numerical errors.
      t = -1;
    }
    return Math.exp(helper1(t) * x);
  }

  private static double helper1(final double x) {
    if (Math.abs(x) > TAYLOR_THRESHOLD) {
      return Math.log1p(x) / x;
    }
    return 1 - (x * (F_1_2 - (x * (F_1_3 - (F_1_4 * x)))));
  }

  private static double helper2(final double x) {
    if (Math.abs(x) > TAYLOR_THRESHOLD) {
      return Math.expm1(x) / x;
    }
    return 1 + (x * F_1_2 * (1 + (x * F_1_3 * (1 + (F_1_4 * x)))));
  }

}
