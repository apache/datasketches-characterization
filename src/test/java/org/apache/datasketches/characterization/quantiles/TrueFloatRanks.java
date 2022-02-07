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

import java.util.Arrays;

import org.apache.datasketches.BinarySearch;
import org.apache.datasketches.characterization.Shuffle;
import org.testng.annotations.Test;

/**
 * Given an array of values, these methods compute the true rank (mass) of
 * each value of the array based on the comparison criterion.
 * The mass or rank of each value is the fractional number of elements of the array that satisfy
 * the criterion.
 *
 * @author Lee Rhodes
 */
public class TrueFloatRanks {
  private static final String LS = System.getProperty("line.separator");
  private boolean ltEq;
  private int length;
  private float[] stream;
  private float[] sortedStream;
  private int[] sortedAbsRanks;
  private int[] streamAbsRanks; //TODO do we need this?

  TrueFloatRanks() { } //for TestNG

  public TrueFloatRanks(final float[] stream, final boolean ltEq) {
    this.stream = stream;
    this.ltEq = ltEq;
    compute();
  }

  public float getMinValue() { return sortedStream[0]; }

  public float getMaxValue() { return sortedStream[length - 1]; }

  public int getMinAbsRank() { return sortedAbsRanks[0]; }

  public int getMaxAbsRank() { return sortedAbsRanks[length - 1]; }

  public float[] getStream() { return stream; }

  public float[] getSortedStream() { return sortedStream; }

  public int[] getSortedAbsRanks() { return sortedAbsRanks; }

  public int[] getStreamAbsRanks() { return streamAbsRanks; }

  public double[] getSortedRelRanks() {
    return relativeRank(sortedAbsRanks);
  }

  public double[] getStreamRelRanks() {
    return relativeRank(streamAbsRanks);
  }

  public int getAbsRank(final float v) {
    int idx = BinarySearch.find(sortedStream, 0, length - 1, v);
    return sortedAbsRanks[idx];
  }

  /**
   * Sorts the stream, then computes the sortedAbsRanks based on the comparison criterion.
   */
  private void compute() {
    length = stream.length;
    sortedStream = stream.clone();
    Arrays.sort(sortedStream);
    sortedAbsRanks = new int[length];
    if (ltEq) { //LE
      sortedAbsRanks[length - 1] = length;
      int i = length - 2;
      while (i >= 0) { //goes backwards
        if (sortedStream[i] == sortedStream[i + 1]) { sortedAbsRanks[i] = sortedAbsRanks[i + 1]; }
        else { sortedAbsRanks[i] = i + 1; }
        i--;
      }
    } else { // LT
      sortedAbsRanks[0] = 0;
      int i = 1;
      while (i < length) { //forwards
        if (sortedStream[i - 1] == sortedStream[i]) { sortedAbsRanks[i] = sortedAbsRanks[i - 1]; }
        else { sortedAbsRanks[i] = i; }
        i++;
      }
    }
    streamAbsRanks = new int[length]; //put the ranks in original stream order
    for (int j = 0; j < length; j++) {
      final int idx = BinarySearch.find(sortedStream, 0, length - 1, stream[j]);
      streamAbsRanks[j] = sortedAbsRanks[idx];
    }
  }

  /**
   * Converts an absolute rank array to a relative rank array.
   * @param absRankArr the absolute rank array to be converted.
   * @return the relative rank array.
   */
  public static double[] relativeRank(final int[] absRankArr) {
    int length = absRankArr.length;
    double[] relRank = new double[length];
    for (int i = 0; i < length; i++) { relRank[i] = (double)absRankArr[i] / length; }
    return relRank;
  }

  @Test
  public void checkRanks() {
    final float[] vArr = { 5, 5, 5, 6, 6, 6, 7, 8, 8, 8 };
    checkRanksImpl(vArr);
    println(LS + "SHUFFLED:");
    Shuffle.shuffle(vArr);
    checkRanksImpl(vArr);
  }

  private static void checkRanksImpl(final float[] vArr) {
    StringBuilder sb = new StringBuilder();
    String ffmt  = "%5.1f ";
    String dfmt    = "%5d ";
    TrueFloatRanks trueRanks;

    int N = vArr.length;
    sb.append("Values:").append(LS);
    for (int i = 0; i < N; i++) { sb.append(String.format(ffmt, vArr[i])); }
    sb.append(LS);

    trueRanks = new TrueFloatRanks(vArr, false);
    sb.append("LT Abs Ranks:").append(LS);
    int[] absArr = trueRanks.getStreamAbsRanks();
    for (int i = 0; i < N; i++) { sb.append(String.format(dfmt, absArr[i])); }
    sb.append(LS);
    sb.append("LT Rel Ranks:").append(LS);
    double[] relArr = relativeRank(absArr);
    for (int i = 0; i < N; i++) { sb.append(String.format(ffmt, relArr[i])); }
    sb.append(LS);

    trueRanks = new TrueFloatRanks(vArr, true);
    sb.append("LE Abs Ranks:").append(LS);
    absArr = trueRanks.getStreamAbsRanks();
    for (int i = 0; i < N; i++) { sb.append(String.format(dfmt, absArr[i])); }
    sb.append(LS);
    sb.append("LE Rel Ranks:").append(LS);
    relArr = relativeRank(absArr);
    for (int i = 0; i < N; i++) { sb.append(String.format(ffmt, relArr[i])); }
    sb.append(LS);
    println(sb.toString());
  }

  private static void println(Object o) {
    System.out.println(o.toString());
  }

}
