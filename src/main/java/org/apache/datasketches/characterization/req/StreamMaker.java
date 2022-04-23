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

package org.apache.datasketches.characterization.req;

import static org.apache.datasketches.characterization.req.StreamMaker.Pattern.FLIP_FLOP;
import static org.apache.datasketches.characterization.req.StreamMaker.Pattern.RANDOM;
import static org.apache.datasketches.characterization.req.StreamMaker.Pattern.REVERSED;
import static org.apache.datasketches.characterization.req.StreamMaker.Pattern.SORTED;
import static org.apache.datasketches.characterization.req.StreamMaker.Pattern.SQRT;
import static org.apache.datasketches.characterization.req.StreamMaker.Pattern.ZOOM_IN;
import static org.apache.datasketches.characterization.req.StreamMaker.Pattern.ZOOM_OUT;

import org.apache.datasketches.characterization.Shuffle;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class StreamMaker {
  static final String LS = System.getProperty("line.separator");
  static String TAB = "\t";

  public enum Pattern { SORTED, REVERSED, ZOOM_IN, ZOOM_OUT, RANDOM, SQRT, FLIP_FLOP,
    CLUSTERED, CLUSTERED_ZOOM_IN, ZOOM_IN_SQRT }

  public float min = 0;
  public float max = 0;

  public float[] makeStream(final int n, final Pattern pattern, final int offset) {
    float[] arr = new float[n];
    min = offset;
    max = n - 1 + offset;
    switch (pattern) {
      case SORTED: {
        for (int i = 0; i < n; i++) { arr[i] = i + offset; }
        break;
      }
      case REVERSED: {
        for (int i = 0; i < n; i++) { arr[n - 1 - i] = i + offset; }
        break;
      }
      case ZOOM_IN: {
        for (int i = 0, j = 0; i < n; i++) {
          if ((i & 1) > 0) { arr[i] = n - j - 1 + offset; j++; } //odd
          else { arr[i] = j + offset; }
        }
        break;
      }
      case ZOOM_OUT: {
        for (int i = 0, j = 0; i < n; i++) {
          if ((i & 1) > 0) { arr[n - 1 - i] = n - j - 1 + offset; j++; } //odd
          else { arr[n - 1 - i] = j + offset; }
        }
        break;
      }
      case RANDOM: {
        for (int i = 0; i < n; i++) { arr[i] = i + offset; }
        Shuffle.shuffle(arr);
        break;
      }
      case SQRT: {
        int idx = 0;
        final int t = (int)Math.sqrt(2 * n);
        int item = 0;
        int initialItem = 0;
        int initialSkip = 1;
        for (int i = 0; i < t; i++) {
          item = initialItem;
          int skip = initialSkip;
          for (int j = 0; j < t - i; j++) {
            if (idx > n - 1) { break; }
            arr[idx++] = item + offset;
            item += skip;
            skip += 1;
          }
          if (idx > n - 1) { break; }
          initialSkip += 1;
          initialItem += initialSkip;
        }
        break;
      }
      case ZOOM_IN_SQRT: {
        final int t = (int)Math.floor(Math.sqrt(n));
        int i = 0;
        for (int j = 0; j < t - 1; j++) {
          arr[i] = j + offset; i++;
          for (int k = 0; k < t; k++) {
            arr[i] = (t - j) * t - k - 1 + offset; i++;
          }
        }
        arr[i] = t - 1 + offset;
        break;
      }
      case FLIP_FLOP: {
        final FlipFlopStream ffs = new FlipFlopStream(n, offset);
        ffs.flipFlop(1, 1, n * 2 / 5);
        final int m = n / 5;
        ffs.flipFlop(m, 1, m);
        ffs.flipFlop(1, m, m);
        ffs.flipFlop(1, 1, n);
        arr = ffs.getArray();
        break;
      }
      case CLUSTERED: {
        break;
      }
      case CLUSTERED_ZOOM_IN: {
        break;
      }
    }
    return arr;
  }

  public void printStream(final int n, final Pattern order, final int offset) {
    final float[] stream = makeStream(n, order, offset);
    println(order + " n:" + n + " offset: " + offset);
    for (int i = 0; i < stream.length; i++) {
      //if (i != 0 && i % 21 == 0) { println(""); }
      println(i + TAB + (int)stream[i]);
    }
    println("");
  }

  @Test
  public void checkStreamMaker() {
    printStream(100, SORTED, 1);
    printStream(100, REVERSED, 1);
    //printStream(100, ZOOM_IN, 0);
    printStream(100, ZOOM_IN, 1);
    //printStream(100, ZOOM_OUT, 0);
    printStream(100, ZOOM_OUT, 1);
    //printStream(100, RANDOM, 0);
    printStream(100, RANDOM, 1);
    //printStream(100, SQRT, 0);
    printStream(100, SQRT, 1);
    //printStream(100, FLIP_FLOP, 0);
    printStream(100, FLIP_FLOP, 1);
  }

  static void print(final Object o) { System.out.print(o.toString()); }

  static void println(final Object o) { System.out.println(o.toString()); }
}
