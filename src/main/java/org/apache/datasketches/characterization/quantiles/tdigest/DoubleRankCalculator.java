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

package org.apache.datasketches.characterization.quantiles.tdigest;

public class DoubleRankCalculator {

  public enum Mode { Min, Mid, Max }

  private final double[] values;
  private final Mode mode;
  private int nLess;
  private int nLessOrEq;

  // assumes that values are sorted
  public DoubleRankCalculator(final double[] values, final Mode mode) {
    this.values = values;
    this.mode = mode;
  }

  public double getRank(final double value) {
    if (Mode.Min.equals(mode) || Mode.Mid.equals(mode)) {
      while ((nLess < values.length) && (values[nLess] < value)) {
        nLess++;
      }
    }
    if (Mode.Max.equals(mode) || Mode.Mid.equals(mode)) {
      while ((nLessOrEq < values.length) && (values[nLessOrEq] <= value)) {
        nLessOrEq++;
      }
    }
    if (Mode.Min.equals(mode)) { return (double) nLess / values.length; }
    if (Mode.Max.equals(mode)) { return (double) nLessOrEq / values.length; }
    return (nLess + nLessOrEq) / 2.0 / values.length;
  }

}
