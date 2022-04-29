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

package org.apache.datasketches.characterization.hash;

import org.apache.datasketches.hash.MurmurHash3;
import org.apache.datasketches.hash.XxHash;
import org.apache.datasketches.memory.MurmurHash3v2;

/**
 * @author Lee Rhodes
 */
public class HashLongsSpeedProfile extends BaseHashSpeedProfile {
  private long[] hashOut = new long[2];
  private long[] hashIn = new long[1];

  public HashLongsSpeedProfile() {
    p = new Point(0,0);
  }

  @Override
  void configure() {

  }

  @Override
  void close() { }

  class Point extends BasePoint {
    long sumTrialsMmmHash_nS = 0;
    long sumTrialsOtherHash_nS = 0;
    long sumTrialsOldHash_nS = 0;
    long sumHash = 0;

    Point(final int arrLongs, final int trials) {
      super(arrLongs, trials);
    }

    @Override
    public void reset(final int x, final int trials) {
      this.x = x;
      this.trials = trials;
      sumTrials_nS = 0;
      sumTrialsMmmHash_nS = 0;
      sumTrialsOtherHash_nS = 0;
      sumTrialsOldHash_nS = 0;
      sumHash = 0;
    }

    @Override
    public String getHeader() {
      final String s =
            "  LgLongs" + TAB
          + "    Longs" + TAB
          + "   Trials" + TAB
          + " Total_mS" + TAB
          + " MmmH3_nS" + TAB
          + "   XxH_nS" + TAB
          + "  MMH3_nS" + TAB
          + "SumHash";
      return s;
    }

    @Override
    public String getRow() {
      final double trialsLongs = trials * x;

      final double lgLongsX = Math.log(x) / LN2;
      final double total_mS = sumTrials_nS / 1e6;
      final double avgMmmH_nS = sumTrialsMmmHash_nS / trialsLongs;
      final double avgOther_nS = sumTrialsOtherHash_nS / trialsLongs;
      final double avgOld_nS = sumTrialsOldHash_nS / trialsLongs;

      final String str = String.format(
          "%9.2f\t"  //LgLongs
        + "%9d\t"    //Longs x
        + "%9d\t"    //Trials
        + "%9.3f\t"  //Total ms
        + "%9.3f\t"  //MmmH_nS
        + "%9.3f\t"  //Other_nS
        + "%9.3f\t"  //Old_nS
        + "%16s",    //sumHash
          lgLongsX,
          x,
          trials,
          total_mS,
          avgMmmH_nS,
          avgOther_nS,
          avgOld_nS,
          Long.toHexString(sumHash)
          );
      return str;
    }
  }

  @Override
  void doTrial() {
    final long startTrial_nS = System.nanoTime();
    final long seed = 0L;
    final int longsX = p.x;
    long myVin = vIn;
    long start;
    long stop;
    long memHash = 0; //checksums
    long otherHash = 0;
    long oldHash = 0;
    long memHash_nS = 0;
    long otherHash_nS = 0;
    long oldHash_nS = 0;

    start = System.nanoTime();
    for (long i = 0; i < longsX; i++) {
      memHash += MurmurHash3v2.hash(myVin++, seed, hashOut)[0];
    }
    stop = System.nanoTime();
    memHash_nS = stop - start;

    start = stop;
    for (long i = 0; i < longsX; i++) {
      otherHash += XxHash.hash(myVin++, seed);
    }
    stop = System.nanoTime();
    otherHash_nS = stop - start;

    start = stop;
    for (long i = 0; i < longsX; i++) {
      hashIn[0] = myVin++;
      oldHash += MurmurHash3.hash(hashIn, seed)[0];
    }
    stop = System.nanoTime();
    oldHash_nS = stop - start;

    assert memHash == oldHash;

    ((Point)p).sumHash += otherHash + memHash + oldHash;

    ((Point)p).sumTrialsMmmHash_nS += memHash_nS;
    ((Point)p).sumTrialsOtherHash_nS += otherHash_nS;
    ((Point)p).sumTrialsOldHash_nS += oldHash_nS;
    vIn = myVin;

    p.sumTrials_nS += System.nanoTime() - startTrial_nS;
  }

}
