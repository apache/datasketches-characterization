/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.memory;

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
