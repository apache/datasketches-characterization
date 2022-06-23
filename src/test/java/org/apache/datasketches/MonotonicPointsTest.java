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

import static org.testng.Assert.assertEquals;
import static org.apache.datasketches.MonotonicPoints.*;
import static org.apache.datasketches.Util.*;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class MonotonicPointsTest {


  @Test
  public void checkMonotonicPoints() {
    double[] arr = MonotonicPoints.evenlySpaced(0.0, 100.0, 21, false);
    for (int i = 0; i < arr.length; i++) { println(arr[i] + ""); }
  }

  @Test
  public void checkMonotonicPoints2() {
    double[] arr = MonotonicPoints.evenlySpaced(0, 1, 3, false);
    assertEquals(arr[0], 0.0);
    assertEquals(arr[1], 0.5);
    assertEquals(arr[2], 1.0);
    arr = MonotonicPoints.evenlySpaced(3, 7, 3, false);
    assertEquals(arr[0], 3.0);
    assertEquals(arr[1], 5.0);
    assertEquals(arr[2], 7.0);
  }

  @Test
  public void checkEvenlySpacedPoints() {
    double[] arr = Util.evenlySpaced(0.0, 100.0, 21);
    for (int i = 0; i < arr.length; i++) { println(arr[i] + ""); }
  }

  @Test
  public void checkCountPoints() {
    int lgStart = 0;
    int start = 1 << lgStart;
    int lgEnd = 4;
    int end = 1 << lgEnd;
    final int p = countPoints(lgStart, lgEnd, 4);
    println(p);
    println("-");
    int q = start;
    while (q <= end) {
      println(q);
      q = pwr2SeriesNext(4, q);
    }
  }

  @Test
  public void checkCountLog10Points() {
    int log10Start = 0;
    int start = 1;
    int log10End = 2;
    int end = 100;
    int ppb = 4;
    final int p = countLog10Points(log10Start, log10End, ppb);
    println(p);
    println("-");
    int q = start;
    while (q <= end) {
      println(q);
      q = (int)powerSeriesNextDouble(ppb, q, true, 10.0);
    }
  }

  static void println(Object o) { System.out.println(o.toString()); }
}
