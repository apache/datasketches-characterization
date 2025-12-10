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

import java.util.Arrays;
import java.util.Comparator;

/**
 * A triple (double value, int index, int rank) with sorting.
 */
public class DoubleIntIntTriple {
  public double value;
  public int index;
  public int rank;
  private static final CompareValue compValue = new CompareValue();
  private static final CompareIndex compIndex = new CompareIndex();

  public static final DoubleIntIntTriple[] sortByValue(final DoubleIntIntTriple[] array) {
    Arrays.sort(array, compValue);
    return array;
  }

  public static final DoubleIntIntTriple[] sortByIndex(final DoubleIntIntTriple[] array) {
    Arrays.sort(array, compIndex);
    return array;
  }

  public static final double[] getValueArray(final DoubleIntIntTriple[] array) {
    final int len = array.length;
    final double[] out = new double[len];
    for (int i = 0; i < len; i++) { out[i] = array[i].value; }
    return out;
  }

  public static final int[] getRankArray(final DoubleIntIntTriple[] array) {
    final int len = array.length;
    final int[] out = new int[len];
    for (int i = 0; i < len; i++) { out[i] = array[i].rank; }
    return out;
  }

  private static final class CompareValue implements Comparator<DoubleIntIntTriple> {
    @Override
    public int compare(final DoubleIntIntTriple t1, final DoubleIntIntTriple t2) {
      return Double.compare(t1.value, t2.value); // handles NaN, +/- 0, etc.
    }
  }

  private static final class CompareIndex implements Comparator<DoubleIntIntTriple> {
    @Override
    public int compare(final DoubleIntIntTriple t1, final DoubleIntIntTriple t2) {
      return Integer.compare(t1.index, t2.index);
    }
  }

}
