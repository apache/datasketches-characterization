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
import static java.lang.Math.pow;
import static org.apache.datasketches.Util.evenlySpaced;
import static org.apache.datasketches.Util.evenlySpacedFloats;

/**
 * @author Lee Rhodes
 */
public class ExponentiallySpacedPoints {

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

  static void println(final Object o) { System.out.println(o.toString()); }
}
