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
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.WritableMemory;

public class HllUnionUpdateSpeedProfile extends BaseUpdateSpeedProfile {
  private int lgK;
  private TgtHllType tgtHllType;
  private int numSketches;
  private HllSketch[] sketches;
  private boolean direct;

  @Override
  public void configure() {
    lgK = Integer.parseInt(prop.mustGet("LgK"));
    tgtHllType = TgtHllType.valueOf(prop.mustGet("TgtHllType"));

    numSketches = Integer.parseInt(prop.mustGet("NumSketches"));
    sketches = new HllSketch[numSketches];
    direct = Boolean.parseBoolean(prop.mustGet("Direct"));
  }

  @Override
  public double doTrial(final int uPerTrial) {
    if (direct) {
      final int bytes = HllSketch.getMaxUpdatableSerializationBytes(lgK, tgtHllType);
      for (int i = 0; i < numSketches; i++) {
        final WritableMemory wmem = WritableMemory.allocate(bytes);
        sketches[i] = new HllSketch(lgK, tgtHllType, wmem);
      }
    } else {
      for (int i = 0; i < numSketches; i++) {
        sketches[i] = new HllSketch(lgK, tgtHllType);
      }
    }

    { // spray values across all sketches
      // if uPerTrial < numSketches, some sketches will be empty.
      // if (uPerTrial % numSketches != 0) some sketches will have one less update.
      int i = 0;
      for (int u = uPerTrial; u-- > 0;) {
        sketches[i++].update(++vIn);
        if (i == numSketches) { i = 0; }
      }
    }

    final Union union = new Union(lgK);
    final long startUpdateTime_nS = System.nanoTime();

    for (int i = numSketches; i-- > 0;) {
      union.update(sketches[i]);
    }

    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return updateTime_nS;
  }

}
