/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.hash;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.hash.MemoryMurmurHash3;
import com.yahoo.sketches.hash.MurmurHash3;

/**
 * @author Lee Rhodes
 */
public class MemoryMurmurHash3SpeedProfile extends BaseBlockHashSpeedProfile {
  long[] in;
  WritableMemory wmem;
  final long[] out = new long[2];

  @Override
  void configure() { //for all trials at a point
    in = new long[p.longsX];
    wmem = WritableMemory.wrap(in);
  }

  @Override
  void close() { }

  @Override
  void doTrial() {
    final int longsX = p.longsX;
    final long startTrial_nS = System.nanoTime();
    long myVin = vIn;
    long start;
    long stop;
    long memHash = 0; //checksums
    long arrHash = 0;
    long oldHash = 0;
    long fill_nS = 0;
    long memHash_nS = 0;
    long arrHash_nS = 0;
    long oldHash_nS = 0;

    start = System.nanoTime(); //fill
    for (int i = 0; i < longsX; i++) {
      in[i] = myVin++;
    }
    stop = System.nanoTime();
    fill_nS = stop - start;

    start = System.nanoTime(); //Memory hash
    memHash = MemoryMurmurHash3.hash(wmem, 0, longsX << 3, 0L, out)[0];
    stop = System.nanoTime();
    memHash_nS = stop - start;

    start = System.nanoTime(); //long[] hash
    arrHash = MemoryMurmurHash3.hash(in, 0, longsX, 0L, out)[0];
    stop = System.nanoTime();
    arrHash_nS = stop - start;

    start = System.nanoTime(); //old hash
    oldHash = MurmurHash3.hash(in, 0)[0];
    stop = System.nanoTime();
    oldHash_nS = stop - start;

    p.sumTrials_nS += System.nanoTime() - startTrial_nS;

    //update parent
    trialMemHash = memHash;
    trialArrHash = arrHash;
    trialOldHash = oldHash;

    vIn = myVin;
    p.sumTrialsFill_nS += fill_nS;
    p.sumTrialsMemHash_nS += memHash_nS;
    p.sumTrialsArrHash_nS += arrHash_nS;
    p.sumTrialsOldHash_nS += oldHash_nS;
  }
}
