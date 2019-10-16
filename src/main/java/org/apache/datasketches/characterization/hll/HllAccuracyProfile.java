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

import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.AccuracyStats;
import org.apache.datasketches.characterization.uniquecount.BaseAccuracyProfile;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;

public class HllAccuracyProfile extends BaseAccuracyProfile {
  private HllSketch sketch;
  private boolean useComposite; //accuracy, HLL
  private boolean useCharArr; //accuracy ?? or speed HLL, Theta?

  @Override
  public void configure() {
    //Configure Sketch
    final boolean direct = Boolean.parseBoolean(prop.mustGet("HLL_direct"));
    useComposite = Boolean.parseBoolean(prop.mustGet("HLL_useComposite"));
    final String useCharArrStr = prop.get("Trials_charArr");
    useCharArr = (useCharArrStr == null) ? false : Boolean.parseBoolean(useCharArrStr);

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
    int lastUniques = 0;
    final int sw = (useCharArr ? 2 : 0) | (useComposite ? 1 : 0);
    switch (sw) {
      case 0: { //use longs; use HIP
        for (int i = 0; i < qArrLen; i++) {
          final AccuracyStats q = qArr[i];
          final double delta = q.trueValue - lastUniques;
          for (int u = 0; u < delta; u++) {
            sketch.update(++vIn);
          }
          lastUniques += delta;
          final double est = sketch.getEstimate();
          q.update(est);
          if (getSize) {
            q.bytes = sketch.toCompactByteArray().length;
          }
        }
        break;
      }
      case 1: { //use longs; use Composite
        for (int i = 0; i < qArrLen; i++) {
          final AccuracyStats q = qArr[i];
          final double delta = q.trueValue - lastUniques;
          for (int u = 0; u < delta; u++) {
            sketch.update(++vIn);
          }
          lastUniques += delta;
          final double est = sketch.getCompositeEstimate();
          q.update(est);
          if (getSize) {
            q.bytes = sketch.toCompactByteArray().length;
          }
        }
        break;
      }
      case 2: { //use char[]; use HIP
        for (int i = 0; i < qArrLen; i++) {
          final AccuracyStats q = qArr[i];
          final double delta = q.trueValue - lastUniques;
          for (int u = 0; u < delta; u++) {
            final String vstr = Long.toHexString(++vIn);
            sketch.update(vstr.toCharArray());
          }
          lastUniques += delta;
          final double est = sketch.getEstimate();
          q.update(est);
          if (getSize) {
            q.bytes = sketch.toCompactByteArray().length;
          }
        }
        break;
      }
      case 3: { //use char[]; use Composite
        for (int i = 0; i < qArrLen; i++) {
          final AccuracyStats q = qArr[i];
          final double delta = q.trueValue - lastUniques;
          for (int u = 0; u < delta; u++) {
            final String vstr = Long.toHexString(++vIn);
            sketch.update(vstr.toCharArray());
          }
          lastUniques += delta;
          final double est = sketch.getCompositeEstimate();
          q.update(est);
          if (getSize) {
            q.bytes = sketch.toCompactByteArray().length;
          }
        }
        break;
      }
    }
  }

}
