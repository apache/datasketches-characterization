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

package org.apache.datasketches.characterization.theta;

import org.apache.datasketches.Family;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.characterization.uniquecount.BaseSerDeProfile;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;

/**
 * @author Lee Rhodes
 */
public class ThetaSerDeProfile extends BaseSerDeProfile {
  private UpdateSketch sketch;

  @Override
  public void configure() {
    //Configure Sketch
    final Family family = Family.stringToFamily(prop.mustGet("THETA_famName"));
    final float p = Float.parseFloat(prop.mustGet("THETA_p"));
    final ResizeFactor rf = ResizeFactor.getRF(Integer.parseInt(prop.mustGet("THETA_lgRF")));
    final boolean direct = Boolean.parseBoolean(prop.mustGet("THETA_direct"));

    final int k = 1 << lgK;
    final UpdateSketchBuilder udBldr = UpdateSketch.builder()
        .setNominalEntries(k)
        .setFamily(family)
        .setP(p)
        .setResizeFactor(rf);
    if (direct) {
      final int bytes = Sketch.getMaxUpdateSketchBytes(k);
      final byte[] memArr = new byte[bytes];
      final WritableMemory wmem = WritableMemory.wrap(memArr);
      sketch = udBldr.build(wmem);
    } else {
      sketch = udBldr.build();
    }
  }

  @Override
  public void doTrial(final Stats stats, final int uPerTrial) {
    sketch.reset(); // reuse the same sketch

    for (int u = uPerTrial; u-- > 0;) { //populate the sketch
      sketch.update(++vIn);
    }
    final double est1 = sketch.getEstimate();

    final long startSerTime_nS = System.nanoTime();
    final byte[] byteArr = sketch.toByteArray();

    final long startDeSerTime_nS = System.nanoTime();

    final UpdateSketch sketch2 = UpdateSketch.heapify(Memory.wrap(byteArr));
    final long endDeTime_nS = System.nanoTime();

    final double est2 = sketch2.getEstimate();
    assert est1 == est2;

    stats.serializeTime_nS = startDeSerTime_nS - startSerTime_nS;
    stats.deserializeTime_nS = endDeTime_nS - startDeSerTime_nS;
    stats.size_bytes = byteArr.length;
  }

}
