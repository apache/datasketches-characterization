/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.memory;

import static com.yahoo.memory.UnsafeUtil.unsafe;

/**
 * @author Lee Rhodes
 */
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
