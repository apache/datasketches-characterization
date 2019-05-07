/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.theta;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.characterization.uniquecount.BaseSerDeProfile;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.UpdateSketchBuilder;

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
  }

}
