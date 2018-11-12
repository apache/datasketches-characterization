/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.hash;

import static com.yahoo.sketches.hash.MemoryMurmurHash3.hash;

import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class MemoryMurmurHash3SpeedProfile extends BaseBlockHashSpeedProfile {
  long[] in;
  WritableMemory wmem = WritableMemory.wrap(in);
  final long[] out = new long[2];

  @Override
  void configure() { //for all trials
    in = new long[p.longsX];
  }

  @Override
  void close() { }

  @Override
  void doTrial() {
    final int longsX = p.longsX;
    final long startTrial_nS = System.nanoTime();
    long start;
    long stop;
    long trialSumMem = 0;
    long trialSumArr = 0;

    start = System.nanoTime(); //fill
    for (int i = 0; i < longsX; i++) {
      in[i] = vIn++;
    }
    stop = System.nanoTime();
    p.sumTrialsFill_nS += stop - start;

    start = System.nanoTime(); //Memory hash
    for (long i = 0; i < longsX; i++) {
      trialSumMem += hash(wmem, 0, longsX << 3, 0L, out)[0];
    }
    stop = System.nanoTime();
    p.sumTrialsMemHash_nS += stop - start;

    start = System.nanoTime(); //long[] hash
    for (long i = 0; i < longsX; i++) {
      trialSumArr += hash(in, 0, longsX, 0L, out)[0];
    }
    stop = System.nanoTime();
    p.sumTrialsArrHash_nS += stop - start;

    p.sumTrials_nS += System.nanoTime() - startTrial_nS;
    if (trialSumMem != trialSumArr) {
      throw new IllegalStateException("Hash sums do not match!");
    }
    return;
  }
}
