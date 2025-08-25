/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches;

import static java.lang.Math.E;
import static java.lang.Math.exp;
import static java.lang.Math.log;
import static java.lang.Math.pow;

import org.apache.datasketches.common.SketchesArgumentException;

/**
 * @author Lee Rhodes
 */
public class SpacedPoints {
  private static final double LOG2 = log(2.0);

  /**
   * Generates a double[] of monotonic points that are exponentially distributed between the
   * endpoints x1 and x2.
   * @param x1 The first point
   * @param x2 The last point
   * @param numPoints must be greater than 2
   * @param exponent must be positive and &ge; 1.0
   * @param denseHigh if true, the highest density of points will be close to x2, otherwise x1.
   * @return the exponentially distributed points.
   */
  public static double[] expSpaced(final double x1, final double x2, final int numPoints,
      final double exponent, final boolean denseHigh) {
    final double[] outArr = evenlySpaced(0.0, 1.0, numPoints);
    final double range = x2 - x1;

    if (denseHigh) {
      for (int i = 0; i < numPoints; i++) {
        final double y = 1.0 - ( exp(pow(1.0 - outArr[i], exponent)) - 1.0) / (E - 1.0);
        outArr[i] = y * range + x1;
      }
    } else { //denseLow
      for (int i = 0; i < numPoints; i++) {
        final double y = (exp(pow(outArr[i], exponent)) - 1.0) / (E - 1.0);
        outArr[i] = y * range + x1;
      }
    }
    return outArr;
  }

  /**
   * Generates a float[] of monotonic points that are exponentially distributed between the
   * endpoints x1 and x2.
   * @param x1 The first point
   * @param x2 The last point
   * @param numPoints must be greater than 2
   * @param exponent must be positive and &ge; 1.0
   * @param denseHigh if true, the highest density of points will be close to x2, otherwise x1.
   * @return the exponentially distributed points.
   */
  public static float[] expSpacedFloats(final double x1, final double x2, final int numPoints,
      final double exponent, final boolean denseHigh) {
    final float[] outArr = evenlySpacedFloats(0.0f, 1.0f, numPoints);
    final double range = x2 - x1;

    if (denseHigh) {
      for (int i = 0; i < numPoints; i++) {
        final double y = 1.0 - ( exp(pow(1.0 - outArr[i], exponent)) - 1.0) / (E - 1.0);
        outArr[i] = (float)(y * range + x1);
      }
    } else { //denseLow
      for (int i = 0; i < numPoints; i++) {
        final double y = (exp(pow(outArr[i], exponent)) - 1.0) / (E - 1.0);
        outArr[i] = (float)(y * range + x1);
      }
    }
    return outArr;
  }

  /**
   * Returns a double array of evenly spaced values between value1 and value2 inclusive.
   * If value2 &gt; value1, the resulting sequence will be increasing.
   * If value2 &lt; value1, the resulting sequence will be decreasing.
   * @param value1 will be in index 0 of the returned array
   * @param value2 will be in the highest index of the returned array
   * @param num the total number of values including value1 and value2. Must be 2 or greater.
   * @return a double array of evenly spaced values between value1 and value2 inclusive.
   */
  public static double[] evenlySpaced(final double value1, final double value2, final int num) {
    if (num < 2) {
      throw new SketchesArgumentException("num must be >= 2");
    }
    final double[] out = new double[num];
    out[0] = value1;
    out[num - 1] = value2;
    if (num == 2) { return out; }

    final double delta = (value2 - value1) / (num - 1);

    for (int i = 1; i < num - 1; i++) { out[i] = i * delta + value1; }
    return out;
  }

  /**
   * Returns a float array of evenly spaced values between value1 and value2 inclusive.
   * If value2 &gt; value1, the resulting sequence will be increasing.
   * If value2 &lt; value1, the resulting sequence will be decreasing.
   * @param value1 will be in index 0 of the returned array
   * @param value2 will be in the highest index of the returned array
   * @param num the total number of values including value1 and value2. Must be 2 or greater.
   * @return a float array of evenly spaced values between value1 and value2 inclusive.
   */
  public static float[] evenlySpacedFloats(final float value1, final float value2, final int num) {
    if (num < 2) {
      throw new SketchesArgumentException("num must be >= 2");
    }
    final float[] out = new float[num];
    out[0] = value1;
    out[num - 1] = value2;
    if (num == 2) { return out; }

    final float delta = (value2 - value1) / (num - 1);

    for (int i = 1; i < num - 1; i++) { out[i] = i * delta + value1; }
    return out;
  }

  /**
   * Returns a double array of values between min and max inclusive where the log of the
   * returned values are evenly spaced.
   * If value2 &gt; value1, the resulting sequence will be increasing.
   * If value2 &lt; value1, the resulting sequence will be decreasing.
   * @param value1 will be in index 0 of the returned array, and must be greater than zero.
   * @param value2 will be in the highest index of the returned array, and must be greater than zero.
   * @param num the total number of values including value1 and value2. Must be 2 or greater
   * @return a double array of exponentially spaced values between value1 and value2 inclusive.
   */
  public static double[] evenlyLogSpaced(final double value1, final double value2, final int num) {
    if (num < 2) {
      throw new SketchesArgumentException("num must be >= 2");
    }
    if (value1 <= 0 || value2 <= 0) {
      throw new SketchesArgumentException("value1 and value2 must be > 0.");
    }

    final double[] arr = evenlySpaced(log(value1) / LOG2, log(value2) / LOG2, num);
    for (int i = 0; i < arr.length; i++) { arr[i] = pow(2.0,arr[i]); }
    return arr;
  }

  static void println(final Object o) { System.out.println(o.toString()); }
}
