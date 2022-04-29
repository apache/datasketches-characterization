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

import static org.apache.datasketches.memory.internal.UnsafeUtil.unsafe;

import org.apache.datasketches.hash.XxHash;
import org.apache.datasketches.memory.MurmurHash3v2;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class HashBytesSpeedProfile extends BaseHashSpeedProfile {
  byte[] in;
  WritableMemory wmem;
  final long[] out = new long[2];

  public HashBytesSpeedProfile() {
    p = new Point(0,0);
  }

  @Override
  void configure() { //for all trials at a point
    in = new byte[p.x];
    wmem = WritableMemory.writableWrap(in);
  }

  @Override
  void close() { }

  class Point extends BasePoint {
    long sumTrialsFill_nS = 0;
    long sumTrialsMmmHash_nS = 0;
    long sumTrialsOtherHash_nS = 0;
    long sumHash = 0;

    Point(final int bytesX, final int trials) {
      super(bytesX, trials);
    }

    @Override
    public void reset(final int x, final int trials) {
      this.x = x;
      this.trials = trials;
      sumTrials_nS = 0;
      sumTrialsFill_nS = 0;
      sumTrialsMmmHash_nS = 0;
      sumTrialsOtherHash_nS = 0;
    }

    @Override
    public String getHeader() {
      final String s =
            "  LgBytes" + TAB
          + "    Bytes" + TAB
          + "   Trials" + TAB
          + " Total_mS" + TAB
          + "  Read_nS" + TAB
          + " MmmH3_nS" + TAB
          + "   XxH_nS" + TAB
          + "Read_MB/S" + TAB
          + "MmmH3_MB/S" + TAB
          + " XxH_MB/S" + TAB
          + "SumHash";
      return s;
    }

    @Override
    public String getRow() {
      final double trialsBytes = trials * x;
      final double lgBytesX = Math.log(x) / LN2;
      final double total_mS = sumTrials_nS / 1e6;
      final double avgBytesFill_nS = sumTrialsFill_nS / trialsBytes;
      final double avgBytesMemHash_nS = sumTrialsMmmHash_nS / trialsBytes;
      final double avgBytesOtherHash_nS = sumTrialsOtherHash_nS / trialsBytes;
      final double fillMBPerSec = 1.0 / (avgBytesFill_nS / 1e3);
      final double memHashMBPerSec = 1.0 / (avgBytesMemHash_nS / 1e3);
      final double otherHashMBPerSec = 1.0 / (avgBytesOtherHash_nS / 1e3);
      final String out = String.format(
          "%9.2f\t"  //LgBytes
        + "%9d\t"    //Bytes
        + "%9d\t"    //Trials
        + "%9.3f\t"  //Total ms
        + "%9.3f\t"  //Read ns
        + "%9.3f\t"  //MmmH3 ns
        + "%9.3f\t"  //Other ns
        + "%9.0f\t"  //Read rate
        + "%9.0f\t"  //MmmH3 rate
        + "%10.0f\t"  //Other rate
        + "%16s",    //sumHash
          lgBytesX,
          x,
          trials,
          total_mS,
          avgBytesFill_nS,
          avgBytesMemHash_nS,
          avgBytesOtherHash_nS,
          fillMBPerSec,
          memHashMBPerSec,
          otherHashMBPerSec,
          Long.toHexString(sumHash)
          );
      return out;
    }
  }

  @Override
  void doTrial() {
    final long seed = 0L;
    final int bytesX = p.x;
    final long startTrial_nS = System.nanoTime();
    long myVin = vIn;
    long start;
    long stop;
    long memHash = 0; //checksums
    long otherHash = 0;
    long fill_nS = 0;
    long memHash_nS = 0;
    long otherHash_nS = 0;
    long sumInput = 0;

    for (int i = 0; i < bytesX; i++) {
      in[i] = (byte) myVin++;
    }

    start = System.nanoTime(); //Read
    sumInput = readMem(wmem);
    stop = System.nanoTime();
    fill_nS = stop - start;

    start = stop; //Memory hash
    memHash = MurmurHash3v2.hash(wmem, 0, bytesX, seed, out)[0];
    stop = System.nanoTime();
    memHash_nS = stop - start;

    start = stop; //other hash
    otherHash = XxHash.hash(wmem, 0L, bytesX, seed);
    stop = System.nanoTime();
    otherHash_nS = stop - start;

    p.sumTrials_nS += System.nanoTime() - startTrial_nS;

    //Check that hashes are the same
    //    if (memHash != otherHash) {
    //      throw new IllegalStateException("Mem checksums do not match!");
    //    }
    ((Point)p).sumHash += otherHash + memHash + sumInput;
    ((Point)p).sumTrialsFill_nS += fill_nS;
    ((Point)p).sumTrialsMmmHash_nS += memHash_nS;
    ((Point)p).sumTrialsOtherHash_nS += otherHash_nS;
    vIn = myVin;
  }

  //@SuppressWarnings("restriction")
  private static final long readMem(final WritableMemory wmem) {
    long sumInput = 0;
    long rem = wmem.getCapacity();
    long cumOff = wmem.getCumulativeOffset();
    final Object unsafeObj = wmem.getArray();
    while (rem >= 32L) {
      sumInput += unsafe.getLong(unsafeObj, cumOff);
      sumInput += unsafe.getLong(unsafeObj, cumOff + 8L);
      sumInput += unsafe.getLong(unsafeObj, cumOff + 16L);
      sumInput += unsafe.getLong(unsafeObj, cumOff + 24L);
      cumOff += 32L;
      rem -= 32L;
    }
    while (rem >= 8L) {
      sumInput += unsafe.getLong(unsafeObj, cumOff);
      cumOff += 8L;
      rem -= 8L;
    }
    if (rem >= 4L) {
      sumInput += unsafe.getInt(unsafeObj, cumOff);
      cumOff += 4L;
      rem -= 4L;
    }
    while (rem > 0) {
      sumInput += unsafe.getByte(unsafeObj, cumOff);
      cumOff++;
      rem--;
    }
    return sumInput;
  }
}

