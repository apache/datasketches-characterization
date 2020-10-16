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

package org.apache.datasketches.characterization.quantiles;

import org.testng.annotations.Test;

/**
 * Generate multiple flip-flop sequences with the given N.
 * @author Lee Rhodes
 */
public class FlipFlopStream {
  private float[] arr;
  private int v;
  private int idx;
  private int low;
  private int high;
  private int lo;
  private int hi;

  /**
   * Constuctor used by TestNG
   */
  public FlipFlopStream() {}

  /**
   * Construct an overall sequence of size N
   * @param N the length of the sequence and size of the returned array.
   * @param offset The lowest value in the sequence. Usually either 0 or 1.
   */
  public FlipFlopStream(int N, int offset) {
    arr = new float[N];
    idx = 0;
    v = offset;
    low = 0;
    high = N - 1;
    lo = low;
    hi = high;
  }

  /**
   * Generates a flip-flop sequence
   * @param loReps repetitions for the low range
   * @param hiReps repetitions for the high range
   * @param steps maximum number of steps for this sequence
   */
  public void flipFlop(int loReps, int hiReps, int steps) {
    int n = hi - lo + 1;
    while (n > 0 && steps > 0) {
      int i = loReps;
      while (n > 0 && steps > 0 && i > 0) {
        arr[idx++] = lo++ + v;
        n--;
        steps--;
        i--;
      }
      int j = hiReps;
      while (n > 0 && steps > 0 && j > 0) {
        arr[idx++] = hi-- + v;
        n--;
        steps--;
        j--;
      }
    }
  }

  /**
   * @return the populated array
   */
  public float[] getArray() {
    return arr;
  }

  @Test
  public void checkFlipFlop() {
    int N = 50;
    FlipFlopStream ffs = new FlipFlopStream(N, 1);
    ffs.flipFlop(1, 1, 20);
    ffs.flipFlop(10, 1, 10);
    ffs.flipFlop(1, 10, 10);
    ffs.flipFlop(1, 1, 10);
    for (int i = 0; i < N; i++) { println(ffs.arr[i]); }
  }

  static void println(Object o) { System.out.println(o.toString()); }
}
