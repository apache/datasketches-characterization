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

import static org.apache.datasketches.memory.UnsafeUtil.unsafe;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("restriction")
public class UnsafeDirectSpeedProfile extends BaseSpeedProfile {
  int arrLongs;
  long address;

  @Override
  void configure(final int arrLongs) { //once per X point
    this.arrLongs = arrLongs;
    address = unsafe.allocateMemory(arrLongs << 3);
  }

  @Override
  void close() {
    unsafe.freeMemory(address);
  }

  @Override
  long doTrial(final boolean read) {
    final long checkSum = (arrLongs * (arrLongs - 1L)) / 2L;
    final long startTime_nS, stopTime_nS;
    if (read) {
      long trialSum = 0;

      startTime_nS = System.nanoTime();
      for (int i = 0; i < arrLongs; i++) { trialSum += unsafe.getLong(address + (i << 3)); }
      stopTime_nS = System.nanoTime();

      if (trialSum != checkSum) {
        throw new IllegalStateException("Bad checksum: " + trialSum + " != " + checkSum);
      }
    } else { //write

      startTime_nS = System.nanoTime();
      for (int i = 0; i < arrLongs; i++) { unsafe.putLong(address + (i << 3), i); }
      stopTime_nS = System.nanoTime();

    }
    return stopTime_nS - startTime_nS;
  }

}
