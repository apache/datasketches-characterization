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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.Util.RandomCodePoints;
import org.apache.datasketches.memory.WritableMemory;

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
