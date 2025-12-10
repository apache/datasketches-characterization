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

package org.apache.datasketches.characterization;

import java.util.Arrays;

import org.apache.datasketches.quantilescommon.BinarySearch;
import org.testng.annotations.Test;

/**
 * Given an array of values, these methods compute the true rank (mass) of
 * each value of the array based on the comparison criterion.
 * The mass or rank of each value is the fractional number of elements of the array that satisfy
 * the criterion.
 *
 * @author Lee Rhodes
 */
public class TrueRanks {
  private static final String LS = System.getProperty("line.separator");
  private int length;
  private double[] dStream;
  private float[] fStream;
  private double[] sortedDStream;
  private int[] sortedAbsRanks;
  private int[] streamAbsRanks;
  
  TrueRanks() { } //for TestNG

  public TrueRanks(final double[] stream, final boolean LTEQ) {
    this.dStream = stream;
    this.fStream = toFloatArr(stream);
    compute(LTEQ);
  }

  public TrueRanks(final float[] stream, final boolean LTEQ) {
    this.fStream = stream;
    this.dStream = toDoubleArr(stream);
    compute(LTEQ);
  }
  
  public double getMinDoubleValue() { return sortedDStream[0]; }
  
  public float getMinFloatValue() { return (float) sortedDStream[0]; }

  public double getMaxDoubleValue() { return sortedDStream[length - 1]; }
  
  public float getMaxFloatValue() { return (float) sortedDStream[length - 1]; }

  public int getMinAbsRank() { return sortedAbsRanks[0]; }

  public int getMaxAbsRank() { return sortedAbsRanks[length - 1]; }

  public double[] getDoubleStream() { return dStream; }
  
  public float[] getFloatStream() { return fStream; }

  public double[] getSortedDoubleStream() { return sortedDStream; }
  
  public float[] getSortedFloatStream() { return toFloatArr(sortedDStream); }

  public int[] getSortedAbsRanks() { return sortedAbsRanks; }

  public int[] getStreamAbsRanks() { return streamAbsRanks; }
  
  public double[] getSortedRelRanks() {
    return relativeRank(sortedAbsRanks);
  }

  public double[] getStreamRelRanks() {
    return relativeRank(streamAbsRanks);
  }

  public int getAbsRank(final double v) {
    final int idx = BinarySearch.find(sortedDStream, 0, length - 1, v);
    return sortedAbsRanks[idx];
  }

  private static float[] toFloatArr(final double[] dArr) {
    final int len = dArr.length;
    final float[] fArr = new float[len];
    for (int i = 0; i < len; i++) { fArr[i] = (float) dArr[i]; }
    return fArr;
  }
  
  private static double[] toDoubleArr(final float[] fArr) {
    final int len = fArr.length;
    final double[] dArr = new double[len];
    for (int i = 0; i < len; i++) { dArr[i] = fArr[i]; }
    return dArr;
  }
  
  /**
   * Sorts the stream, then computes the sortedAbsRanks based on the comparison criterion.
   */
  private void compute(final boolean LTEQ) {
    length = dStream.length;
    sortedDStream = dStream.clone();
    Arrays.sort(sortedDStream);
    sortedAbsRanks = new int[length];
    if (LTEQ) { //if LTEQ == true, criteria is <=
      sortedAbsRanks[length - 1] = length;
      int i = length - 2;
      while (i >= 0) { //goes backwards
        if (sortedDStream[i] == sortedDStream[i + 1]) { sortedAbsRanks[i] = sortedAbsRanks[i + 1]; }
        else { sortedAbsRanks[i] = i + 1; }
        i--;
      }
    } else { // LT
      sortedAbsRanks[0] = 0;
      int i = 1;
      while (i < length) { //forwards
        if (sortedDStream[i - 1] == sortedDStream[i]) { sortedAbsRanks[i] = sortedAbsRanks[i - 1]; }
        else { sortedAbsRanks[i] = i; }
        i++;
      }
    }
    streamAbsRanks = new int[length]; //put the ranks in original stream order
    for (int j = 0; j < length; j++) {
      final int idx = BinarySearch.find(sortedDStream, 0, length - 1, dStream[j]);
      streamAbsRanks[j] = sortedAbsRanks[idx];
    }
  }

  /**
   * Converts an absolute rank array to a relative rank array.
   * @param absRankArr the absolute rank array to be converted.
   * @return the relative rank array.
   */
  public static double[] relativeRank(final int[] absRankArr) {
    final int length = absRankArr.length;
    final double[] relRank = new double[length];
    for (int i = 0; i < length; i++) { relRank[i] = (double)absRankArr[i] / length; }
    return relRank;
  }

  @Test
  public void checkRanks() {
    final double[] vArr = { 5, 5, 5, 6, 6, 6, 7, 8, 8, 8 };
    checkRanksImpl(vArr);
    println(LS + "SHUFFLED:");
    Shuffle.shuffle(vArr);
    checkRanksImpl(vArr);
  }

  private static void checkRanksImpl(final double[] vArr) {
    final StringBuilder sb = new StringBuilder();
    final String ffmt  = "%5.1f ";
    final String dfmt    = "%5d ";
    TrueRanks trueRanks;

    final int N = vArr.length;
    sb.append("Values:").append(LS);
    for (int i = 0; i < N; i++) { sb.append(String.format(ffmt, vArr[i])); }
    sb.append(LS);

    trueRanks = new TrueRanks(vArr, false);
    sb.append("LT Abs Ranks:").append(LS);
    int[] absArr = trueRanks.getStreamAbsRanks();
    for (int i = 0; i < N; i++) { sb.append(String.format(dfmt, absArr[i])); }
    sb.append(LS);
    sb.append("LT Rel Ranks:").append(LS);
    double[] relArr = relativeRank(absArr);
    for (int i = 0; i < N; i++) { sb.append(String.format(ffmt, relArr[i])); }
    sb.append(LS);

    trueRanks = new TrueRanks(vArr, true);
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

  private static void println(final Object o) {
    System.out.println(o.toString());
  }

}
