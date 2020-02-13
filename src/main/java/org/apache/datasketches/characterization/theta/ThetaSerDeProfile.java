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

import static org.testng.Assert.assertEquals;

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
  private boolean serde = false;
  private boolean est = false;
  private double err;

  @Override
  public void configure() {
    //Configure Sketch
    final Family family = Family.stringToFamily(prop.mustGet("THETA_famName"));
    final float p = Float.parseFloat(prop.mustGet("THETA_p"));
    final ResizeFactor rf = ResizeFactor.getRF(Integer.parseInt(prop.mustGet("THETA_lgRF")));
    final boolean direct = Boolean.parseBoolean(prop.mustGet("THETA_direct"));
    serde = Boolean.parseBoolean(prop.mustGet("THETA_SerDe"));
    est = Boolean.parseBoolean(prop.mustGet("THETA_Est"));

    final int k = 1 << lgK;
    err = 5.0 / Math.sqrt(k);
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
  public void doTrial(final long[] stats, final int uPerTrial) {
    sketch.reset(); // reuse the same sketch
    double est1 = 0;
    double est2 = 0;
    UpdateSketch sketch2 = null;

    for (int u = uPerTrial; u-- > 0;) { //populate the sketch
      sketch.update(++vIn);
    }
    if (est) {
      final long startEstTime_nS = System.nanoTime();
      est1 = sketch.getEstimate();
      final long stopEstTime_nS = System.nanoTime();

      assertEquals(est1, uPerTrial, uPerTrial * err);
      stats[est_ns] = est ? stopEstTime_nS - startEstTime_nS : 0;
    }
    if (serde) {
      final long startSerTime_nS = System.nanoTime();
      final byte[] byteArr = sketch.toByteArray();

      final long startDeserTime_nS = System.nanoTime();
      sketch2 = UpdateSketch.heapify(Memory.wrap(byteArr));
      final long stopSerDeTime_nS = System.nanoTime();

      est2 = sketch2.getEstimate();
      assertEquals(est2, uPerTrial, uPerTrial * err);
      stats[ser_ns] = serde ? startDeserTime_nS - startSerTime_nS : 0;
      stats[deser_ns] = serde ? stopSerDeTime_nS - startDeserTime_nS : 0;
      stats[size_bytes] = serde ? byteArr.length : 0;
    }
    if (est && serde) {
      assertEquals(est1, est2, 0.0);
    }

  }

}
