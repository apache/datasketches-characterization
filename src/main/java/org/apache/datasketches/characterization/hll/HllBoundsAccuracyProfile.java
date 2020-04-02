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

import org.apache.datasketches.characterization.BoundsAccuracyStats;
import org.apache.datasketches.characterization.uniquecount.BaseBoundsAccuracyProfile;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.memory.WritableMemory;

/**
 * @author Lee Rhodes
 */
public class HllBoundsAccuracyProfile extends BaseBoundsAccuracyProfile {
  private HllSketch sketch;
  private boolean useComposite; //accuracy, HLL

  @Override
  public void configure() {
    //Configure Sketch
    final boolean direct = Boolean.parseBoolean(prop.mustGet("HLL_direct"));
    useComposite = Boolean.parseBoolean(prop.mustGet("HLL_useComposite"));

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
  public void doTrial() {
    final int qArrLen = qArr.length;
    sketch.reset(); //reuse the same sketch
    long lastUniques = 0;
    if (useComposite) {
      for (int i = 0; i < qArrLen; i++) {
        final BoundsAccuracyStats q = qArr[i];
        final long delta = (long)(q.trueValue - lastUniques);
        for (long u = 0; u < delta; u++) {
          sketch.update(++vIn);
        }
        lastUniques += delta;
        final double est = sketch.getCompositeEstimate();
        q.update(est,
            sketch.getLowerBound(3), sketch.getLowerBound(2), sketch.getLowerBound(1),
            sketch.getUpperBound(1), sketch.getUpperBound(2), sketch.getUpperBound(3));
      }
    } else { //use longs; use HIP
      for (int i = 0; i < qArrLen; i++) { //loop through all Trials Points
        final BoundsAccuracyStats q = qArr[i];
        final long delta = (long)(q.trueValue - lastUniques);
        for (long u = 0; u < delta; u++) {
          sketch.update(++vIn);
        }
        lastUniques += delta;
        final double est = sketch.getEstimate();
        q.update(est, sketch.getLowerBound(3), sketch.getLowerBound(2), sketch.getLowerBound(1),
            sketch.getUpperBound(1), sketch.getUpperBound(2), sketch.getUpperBound(3));
      }
    }
  }
}
