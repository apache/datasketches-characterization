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

package org.apache.datasketches.characterization.memory;

/**
 * @author Lee Rhodes
 */
public class HeapSpeedProfile extends BaseSpeedProfile {
  int arrLongs;
  long[] array;

  @Override
  void configure(final int arrLongs) {
    this.arrLongs = arrLongs;
    array = new long[arrLongs];
  }

  @Override
  void close() { }

  @Override
  long doTrial(final boolean read) {
    final long checkSum = (arrLongs * (arrLongs - 1L)) / 2L;
    final long startTime_nS, stopTime_nS;
    if (read) {
      long trialSum = 0;

      startTime_nS = System.nanoTime();
      for (int i = 0; i < arrLongs; i++) { trialSum += array[i]; }
      stopTime_nS = System.nanoTime();

      if (trialSum != checkSum) {
        throw new IllegalStateException("Bad checksum: " + trialSum + " != " + checkSum);
      }
    } else { //write

      startTime_nS = System.nanoTime();
      for (int i = 0; i < arrLongs; i++) { array[i] = i; }
      stopTime_nS = System.nanoTime();

    }
    return stopTime_nS - startTime_nS;
  }

}
