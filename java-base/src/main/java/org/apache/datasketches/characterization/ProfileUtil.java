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

package org.apache.datasketches.characterization;

import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static org.apache.datasketches.common.Util.ceilingPowerBaseOfDouble;
import static org.apache.datasketches.common.Util.logBaseOfX;
import static org.apache.datasketches.common.Util.powerSeriesNextDouble;

/**
 * @author Lee Rhodes
 */
public class ProfileUtil {

  /**
   * Compute the split-points for the PMF function.
   * This assumes all points are positive and may include zero.
   * @param min The data minimum value
   * @param max The data maximum value
   * @param pplb desired number of Points Per Log Base.
   * @param logBase the chosen Log Base
   * @param eps the epsilon added to each split-point
   * @return the split-points array, which may include zero
   */
  public static double[] buildSplitPointsArr(final double min, final double max,
      final int pplb, final double logBase, final double eps) {
    final double ceilPwrBmin = ceilingPowerBaseOfDouble(logBase, min);
    final double ceilPwrBmax = ceilingPowerBaseOfDouble(logBase, max);
    final int bot = (int) ceil(logBaseOfX(logBase, max(1.0, ceilPwrBmin)));
    final int top = (int) ceil(logBaseOfX(logBase, ceilPwrBmax));
    final int numSP;
    double next;
    final double[] spArr;
    if (min < 1.0) {
      numSP = ((top - bot) * pplb) + 2;
      spArr = new double[numSP];
      spArr[0] = 0;
      spArr[1] = 1;
      next = 1.0;
      for (int i = 2; i < numSP; i++) {
        next = powerSeriesNextDouble(pplb, next, false, logBase);
        spArr[i] = next;
      }
    } else {
      numSP = ((top - bot) * pplb) + 1;
      spArr = new double[numSP];
      spArr[0] = ceilPwrBmin;
      next = ceilPwrBmin;
      for (int i = 1; i < numSP; i++) {
        next = powerSeriesNextDouble(pplb, next, false, logBase);
        spArr[i] = next;
      }
    }
    if (eps != 0.0) {
      for (int i = 0; i < numSP; i++) {
        if (spArr[i] == 0.0) { spArr[i] = eps; }
        else { spArr[i] *= 1 + eps; }
      }
    }
    return spArr;
  }

  /**
   * Monotonicity check.
   * @param arr Array to check
   */
  public static void checkMonotonic(final double[] arr) {
    final int len = arr.length;
    for (int i = 1; i < len; i++) {
      assert arr[i] > arr[i - 1];
    }
  }

  //@Test
  public void checkBuildSPArr() {
    final double[] arr = buildSplitPointsArr(0, 999, 2, 10.0, 1E-6);
    for (int i = 0; i < arr.length; i++) { System.out.println("" + arr[i]); }
  }

}
