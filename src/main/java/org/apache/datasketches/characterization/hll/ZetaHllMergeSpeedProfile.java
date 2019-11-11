/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.characterization.hll;

import org.apache.datasketches.characterization.uniquecount.BaseMergeSpeedProfile;

import com.google.zetasketch.HyperLogLogPlusPlus;

/**
 * @author Lee Rhodes
 */
public class ZetaHllMergeSpeedProfile extends BaseMergeSpeedProfile {
  private HyperLogLogPlusPlus.Builder hllBuilder;
  private HyperLogLogPlusPlus<Long> target;

  @Override
  public void configure() {
    hllBuilder = new HyperLogLogPlusPlus.Builder();
  }

  @Override
  public void resetMerge(final int lgK) {
    target = newSketch(lgK);
  }

  private HyperLogLogPlusPlus<Long> newSketch(final int lgK) {
    final int lgSP = Math.min(lgK + 5, 25);
    hllBuilder.normalPrecision(lgK);
    hllBuilder.sparsePrecision(lgSP);
    return hllBuilder.buildForLongs();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void doTrial(final Stats stats, final int lgK, final int lgDeltaU) {
    final int U = 1 << (lgK + lgDeltaU);
    long start;
    long serTime_nS = 0;
    long deserTime_nS = 0;
    long mergeTime_nS = 0;
    final HyperLogLogPlusPlus<Long> source = newSketch(lgK);
    for (int u = 0; u < U; u++) { source.add(++vIn); }
    final HyperLogLogPlusPlus<Long> source2;

    if (serDe) {
      //Serialize
      start = System.nanoTime();
      final byte[] byteArr = source.serializeToByteArray();
      serTime_nS += System.nanoTime() - start;
      //Deserialize
      start = System.nanoTime();
      source2 = (HyperLogLogPlusPlus<Long>) HyperLogLogPlusPlus.forProto(byteArr);
      deserTime_nS += System.nanoTime() - start;
      //Merge
      start = System.nanoTime();
      target.merge(source2);
      mergeTime_nS += System.nanoTime() - start;

    } else {
      //Merge
      start = System.nanoTime();
      target.merge(source);
      mergeTime_nS += System.nanoTime() - start;
    }

    stats.serializeTime_nS = serTime_nS;
    stats.deserializeTime_nS = deserTime_nS;
    stats.mergeTime_nS = mergeTime_nS;
    stats.totalTime_nS = deserTime_nS + mergeTime_nS;
  }

}
