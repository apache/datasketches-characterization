/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.memory;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.yahoo.memory.Memory;
import com.yahoo.memory.Util.RandomCodePoints;
import com.yahoo.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class HeapUtf8SpeedProfile extends BaseUtf8SpeedProfile {
  RandomCodePoints randCP = new RandomCodePoints(false); //only with Memory 0.10.4 and later

  @Override
  void configure() {

  }

  @Override
  void doTrial(final TrialStats stats) {
    final int[] cpArr = stats.cpArr;
    final int cpArrLen = cpArr.length;

    randCP.fillCodePointArray(cpArr);
    final String javaStr = new String(cpArr, 0, cpArrLen); //Java String reference //GG-U
    final int javaStrLen = javaStr.length();
    final byte[] javaByteArr;
    final int javaByteArrLen;
    final WritableMemory wMem;
    long startTime;
    long stopTime;

    //measure Java encode time
    startTime = System.nanoTime();
    javaByteArr = javaStr.getBytes(UTF_8); //Java byteArr reference //GG-U
    stopTime = System.nanoTime();
    stats.javaEncodeTime_nS = stopTime - startTime;

    javaByteArrLen = javaByteArr.length;

    //measure Java decode time
    startTime = System.nanoTime();
    final String javaStr2 = new String(javaByteArr, UTF_8);
    stopTime = System.nanoTime();
    stats.javaDecodeTime_nS = stopTime - startTime;

    checkStrings(javaStr2, javaStr);

    //prepare Memory measurements
    wMem = WritableMemory.allocate(javaByteArrLen); //GG
    final StringBuilder sb = new StringBuilder(javaStrLen); //GG

    //measure Memory encode time
    startTime = System.nanoTime();
    wMem.putCharsToUtf8(0, javaStr);
    stopTime = System.nanoTime();
    stats.memEncodeTime_nS = stopTime - startTime;

    checkMemBytes(Memory.wrap(javaByteArr), wMem); //GG

    //measure Memory decode time
    startTime = System.nanoTime();
    wMem.getCharsFromUtf8(0, javaByteArrLen, sb);
    stopTime = System.nanoTime();
    stats.memDecodeTime_nS = stopTime - startTime;

    checkStrings(sb.toString(), javaStr); //GG-U

  }

  @Override
  void close() {
  }




}
