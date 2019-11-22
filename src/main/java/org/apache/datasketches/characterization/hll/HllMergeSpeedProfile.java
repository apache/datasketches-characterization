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
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class HllMergeSpeedProfile extends BaseMergeSpeedProfile {
  private boolean compact;
  private boolean wrap;
  private boolean direct;
  private TgtHllType tgtHllType;
  private Union union = new Union(21);
  private HllSketch source;

  @Override
  public void configure() {
    direct = Boolean.parseBoolean(prop.mustGet("HLL_direct"));
    compact = Boolean.parseBoolean(prop.mustGet("HLL_compact"));
    wrap = Boolean.parseBoolean(prop.mustGet("HLL_wrap"));

    final String type = prop.mustGet("HLL_tgtHllType");
    if (type.equalsIgnoreCase("HLL4")) { tgtHllType = TgtHllType.HLL_4; }
    else if (type.equalsIgnoreCase("HLL6")) { tgtHllType = TgtHllType.HLL_6; }
    else { tgtHllType = TgtHllType.HLL_8; }
  }

  @Override
  public void resetMerge(final int lgK) {
    union = new Union(lgK);
    source = newSketch(lgK);
    int U = 2 << lgK;
    for (int i = 0; i < U; i++) {
      union.update(++vIn);
      source.update(vIn);
    }
  }

  private HllSketch newSketch(final int lgK) {
    WritableMemory wmem = null;
    final HllSketch sk;
    if (direct) {
      final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, tgtHllType);
      wmem = WritableMemory.allocate(bytes);
      sk = new HllSketch(lgK, tgtHllType, wmem);
    } else {
      sk = new HllSketch(lgK, tgtHllType);
    }
    return sk;
  }


  @Override
  public void doTrial(final Stats stats, final int lgK, final int lgDeltaU) {
    final int U = 1 << (lgK + lgDeltaU);
    long start;
    long serTime_nS = 0;
    long deserTime_nS = 0;
    long mergeTime_nS = 0;
    //final HllSketch source = newSketch(lgK);
    //final long vStartUnion = vIn;
    //final long vStart = vIn;
    //for (int u = 0; u < U; u++) { source.update(++vIn); }
    //final long trueU = vIn - vStart;
    //checkEstimate(trueU, source.getEstimate(), lgK, "Source");
    HllSketch source2 = null;
    final byte[] byteArr;

    if (serDe) {
      //Serialize
      if (compact) {
        start = System.nanoTime();
        byteArr = source.toCompactByteArray();
        serTime_nS += System.nanoTime() - start;
      } else {
        start = System.nanoTime();
        byteArr = source.toUpdatableByteArray();
        serTime_nS += System.nanoTime() - start;
      }
      //Deserialize
      if (wrap) {
        start = System.nanoTime();
        final Memory mem = Memory.wrap(byteArr);
        source2 = HllSketch.wrap(mem);
        deserTime_nS += System.nanoTime() - start;
      } else { //heapify
        start = System.nanoTime();
        final Memory mem = Memory.wrap(byteArr);
        source2 = HllSketch.heapify(mem);
        deserTime_nS += System.nanoTime() - start;
      }
      //checkEstimate(trueU, source2.getEstimate(), lgK, "SerDe");
      //Merge
      start = System.nanoTime();
      union.update(source2);
      mergeTime_nS += System.nanoTime() - start;

    } else {
      //Merge
      start = System.nanoTime();
      union.update(source);
      mergeTime_nS += System.nanoTime() - start;
    }

    stats.serializeTime_nS = serTime_nS;
    stats.deserializeTime_nS = deserTime_nS;
    stats.mergeTime_nS = mergeTime_nS;
    stats.totalTime_nS = mergeTime_nS;

    //final double vUnionActual = vIn - vStartUnion;
    //checkEstimate(vUnionActual, union.getEstimate(), lgK, "Union");

  }

  void checkEstimate(final double actual, final double est, final int lgK, final String note) {
    final double k = 1L << lgK;
    final double bound = 3.0 / Math.sqrt(k);
    final double err = Math.abs((est / actual) - 1.0);
    if (err > bound) {
      System.out.printf("ERROR: %12.3f %12.3f %20s\n", err, bound, note);
    }
  }

}
