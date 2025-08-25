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
 * A triple (float value, int index, int rank) with sorting.
 */
public class FloatIntIntTriple {
  public float value;
  public int index;
  public int rank;
  private static final CompareValue compValue = new CompareValue();
  private static final CompareIndex compIndex = new CompareIndex();
  private static final CompareRank compRank = new CompareRank();

  public static final FloatIntIntTriple[] sortByValue(final FloatIntIntTriple[] array) {
    Arrays.sort(array, compValue);
    return array;
  }

  public static final FloatIntIntTriple[] sortByIndex(final FloatIntIntTriple[] array) {
    Arrays.sort(array, compIndex);
    return array;
  }

  public static final FloatIntIntTriple[] sortByRank(final FloatIntIntTriple[] array) {
    Arrays.sort(array, compRank);
    return array;
  }

  public static final float[] getValueArray(final FloatIntIntTriple[] array) {
    final int len = array.length;
    final float[] out = new float[len];
    for (int i = 0; i < len; i++) { out[i] = array[i].value; }
    return out;
  }

  public static final int[] getRankArray(final FloatIntIntTriple[] array) {
    final int len = array.length;
    final int[] out = new int[len];
    for (int i = 0; i < len; i++) { out[i] = array[i].rank; }
    return out;
  }

  private static final class CompareValue implements Comparator<FloatIntIntTriple> {
    @Override
    public int compare(final FloatIntIntTriple t1, final FloatIntIntTriple t2) {
      return Float.compare(t1.value, t2.value); // handles NaN, +/- 0, etc.
    }
  }

  private static final class CompareIndex implements Comparator<FloatIntIntTriple> {
    @Override
    public int compare(final FloatIntIntTriple t1, final FloatIntIntTriple t2) {
      return Integer.compare(t1.index, t2.index);
    }
  }

  private static final class CompareRank implements Comparator<FloatIntIntTriple> {
    @Override
    public int compare(final FloatIntIntTriple t1, final FloatIntIntTriple t2) {
      return Integer.compare(t1.rank, t2.rank);
    }
  }
}
