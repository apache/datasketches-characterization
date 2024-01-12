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

//import static org.apache.datasketches.Constants.EIGHT_PEBIBYTE;
//import static org.apache.datasketches.Constants.SIXTEEN_MEBIBYTE;
import static java.lang.Math.round;
import static org.apache.datasketches.common.Util.longToFixedLengthString;
import static org.apache.datasketches.common.Util.numDigits;

import java.util.Arrays;
import java.util.Comparator;

import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

/**
 * @author Lee Rhodes
 */
public class QuantilesAccuracyStats {
  public UpdateDoublesSketch qsk;
  public int index; //Used to restore to a given distribution
  public double quantile;     //the true quantile value
  public String strValue = "";     //string equivalent of Quantile
  public long naturalRank;    //in the range [1, n], but not necessarily contiguous
  public double estNormRank;
  public double normRank;
  private static final CompareQuantile compQuantile = new CompareQuantile();
  private static final CompareIndex compIndex = new CompareIndex();

  /**
   * Construct when not using strings
   * @param lgQK the lgK configuration value for the DoublesSketch used for estimating quantiles of the error
   * distribution
   * @param index the index of the array when first created
   * @param quantile the true quantile
   */
  public QuantilesAccuracyStats(final int lgQK, final int index, final double quantile) {
    this.qsk = new DoublesSketchBuilder().setK(1 << lgQK).build(); //for rank estimates
    this.index = index;
    this.quantile = quantile;
  }

  /**
   * Construct when using strings.
   * @param lgQK the lgK configuration value for the DoublesSketch used for estimating quantiles of the error
   * distribution
   * @param index the index of the array when first created
   * @param quantile the true quantile. Values greater than 2^52 will throw an exception.
   * @param maxQ the largest quantile value (not the number of quantiles presented to the target sketch).
   */
  public QuantilesAccuracyStats(final int lgQK, final int index, final double quantile, final double maxQ) {
    this.qsk = new DoublesSketchBuilder().setK(1 << lgQK).build(); //for rank estimates
    this.index = index;
    if (quantile > (1L << 52)) { throw new IllegalArgumentException("Value is too large, > 2^52"); }
    this.quantile = quantile;
    this.strValue = longToFixedLengthString(round(quantile), numDigits(round(maxQ)));
  }

  public static final QuantilesAccuracyStats[] sortByQuantile(final QuantilesAccuracyStats[] array) {
    Arrays.sort(array, compQuantile);
    return array;
  }

  public static final QuantilesAccuracyStats[] sortByIndex(final QuantilesAccuracyStats[] array) {
    Arrays.sort(array, compIndex);
    return array;
  }

  private static final class CompareQuantile implements Comparator<QuantilesAccuracyStats> {
    @Override
    public int compare(final QuantilesAccuracyStats v1, final QuantilesAccuracyStats v2) {
      return Double.compare(v1.quantile, v2.quantile); // handles NaN, +/- 0, etc.
    }
  }

  private static final class CompareIndex implements Comparator<QuantilesAccuracyStats> {
    @Override
    public int compare(final QuantilesAccuracyStats i1, final QuantilesAccuracyStats i2) {
      return Integer.compare(i1.index, i2.index);
    }
  }

}
