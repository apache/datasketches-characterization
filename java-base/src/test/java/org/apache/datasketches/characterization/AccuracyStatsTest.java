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

import static org.apache.datasketches.characterization.AccuracyStats.*;
import org.testng.annotations.Test;

public class AccuracyStatsTest {

  @Test
  public void checkBuildLog10AccuracyStatsArray() {
    final int log10Min = 6;
    final int log10Max = 8;
    final int ppb = 4;
    final int lgQK = 10;
    AccuracyStats[] asArr =
        buildLog10AccuracyStatsArray(log10Min, log10Max, ppb, lgQK);
    println(asArr.length);
  }

  static void println(Object o) { System.out.println(o.toString()); }
}

