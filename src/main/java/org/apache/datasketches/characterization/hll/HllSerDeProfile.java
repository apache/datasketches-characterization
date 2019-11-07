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

package org.apache.datasketches.characterization.hll;

import org.apache.datasketches.characterization.uniquecount.BaseSerDeProfile;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class HllSerDeProfile extends BaseSerDeProfile {
  private HllSketch sketch;

  private boolean compact;
  private boolean wrap;

  @Override
  public void configure() {
    final boolean direct = Boolean.parseBoolean(prop.mustGet("HLL_direct"));
    compact = Boolean.parseBoolean(prop.mustGet("HLL_compact"));
    wrap = Boolean.parseBoolean(prop.mustGet("HLL_wrap"));

    final TgtHllType tgtHllType;
    final String type = prop.mustGet("HLL_tgtHllType");
    if (type.equalsIgnoreCase("HLL4")) { tgtHllType = TgtHllType.HLL_4; }
    else if (type.equalsIgnoreCase("HLL6")) { tgtHllType = TgtHllType.HLL_6; }
    else { tgtHllType = TgtHllType.HLL_8; }

    if (direct) {
      final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, tgtHllType);
      final WritableMemory wmem = WritableMemory.allocate(bytes);
      sketch = new HllSketch(lgK, tgtHllType, wmem);
    } else {
      sketch = new HllSketch(lgK, tgtHllType);
    }
  }

  @Override
  public void doTrial(final Stats stats, final int uPerTrial) {
    sketch.reset(); // reuse the same sketch

    for (int u = uPerTrial; u-- > 0;) {
      sketch.update(++vIn);
    }
    final double est1 = sketch.getEstimate();

    final byte[] byteArr;
    final long startSerTime_nS, stopSerTime_nS;
    if (compact) {
      startSerTime_nS = System.nanoTime();
      byteArr = sketch.toCompactByteArray();
      stopSerTime_nS = System.nanoTime();
    } else {
      startSerTime_nS = System.nanoTime();
      byteArr = sketch.toUpdatableByteArray();
      stopSerTime_nS = System.nanoTime();
    }

    final Memory mem = Memory.wrap(byteArr);
    final HllSketch sketch2;
    final long startDeserTime_nS, stopDeserTime_nS;
    if (wrap) {
      startDeserTime_nS = System.nanoTime();
      sketch2 = HllSketch.wrap(mem);
      stopDeserTime_nS = System.nanoTime();
    } else {
      startDeserTime_nS = System.nanoTime();
      sketch2 = HllSketch.heapify(mem);
      stopDeserTime_nS = System.nanoTime();
    }

    final double est2 = sketch2.getEstimate();
    assert est1 == est2;

    stats.serializeTime_nS = stopSerTime_nS - startSerTime_nS;
    stats.deserializeTime_nS = stopDeserTime_nS - startDeserTime_nS;
    stats.size_bytes = byteArr.length;
  }

}
