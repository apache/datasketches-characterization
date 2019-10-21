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

import org.apache.datasketches.characterization.uniquecount.BaseUpdateSpeedProfile;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.memory.WritableDirectHandle;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class HllUpdateSpeedProfile extends BaseUpdateSpeedProfile {
  private HllSketch sketch;
  private WritableDirectHandle handle;
  private WritableMemory wmem;

  @Override
  public void configure() {
    final int lgK = Integer.parseInt(prop.mustGet("LgK"));
    final boolean offheap = Boolean.parseBoolean(prop.mustGet("HLL_offheap"));

    final TgtHllType tgtHllType;
    final String type = prop.mustGet("HLL_tgtHllType");
    if (type.equalsIgnoreCase("HLL4")) { tgtHllType = TgtHllType.HLL_4; }
    else if (type.equalsIgnoreCase("HLL6")) { tgtHllType = TgtHllType.HLL_6; }
    else { tgtHllType = TgtHllType.HLL_8; }

    if (offheap) {
      final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, tgtHllType);
      handle = WritableMemory.allocateDirect(bytes);
      wmem = handle.get();
      sketch = new HllSketch(lgK, tgtHllType, wmem);
    } else {
      sketch = new HllSketch(lgK, tgtHllType);
    }
  }

  @Override
  public void cleanup() {
    if (handle != null) { handle.close(); }
  }

  @Override
  public double doTrial(final int uPerTrial) {
    sketch.reset(); // reuse the same sketch
    final long startUpdateTime_nS = System.nanoTime();

    for (int u = uPerTrial; u-- > 0;) {
      sketch.update(++vIn);
    }
    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return (double) updateTime_nS / uPerTrial;
  }

}
