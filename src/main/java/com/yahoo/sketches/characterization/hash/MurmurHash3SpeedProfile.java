/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.hash;

import static com.yahoo.sketches.hash.MurmurHash3.hash;

/**
 * @author Lee Rhodes
 */
public class MurmurHash3SpeedProfile extends BaseHashSpeedProfile {
  final long[] in = new long[2];
  final long[] out = new long[2];

  @Override
  void configure() { }

  @Override
  void close() { }

  @Override
  long[] doTrial(final long iterX, final long start) {
    final long startTime_nS;
    final long stopTime_nS;
    final long end = iterX + start;
    long trialSum = 0;
    //in[0] = start;
    startTime_nS = System.nanoTime();
    for (long i = start; i < end; i++) {
      in[0] = i;
      trialSum += hash(in, 0)[0];
    }
    stopTime_nS = System.nanoTime();

    out[0] = stopTime_nS - startTime_nS;
    out[1] = ((trialSum | 1L) > 0L) ? 1L : 0L;
    return out;
  }

}
