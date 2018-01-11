/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.UpdateSketchBuilder;

/**
 * @author Lee Rhodes
 */
public class ThetaUpdateSpeedProfile extends BaseUpdateSpeedProfile {
  private UpdateSketch sketch;

  @Override
  void configure() {
    //Configure Sketch
    final int lgK = Integer.parseInt(prop.mustGet("LgK"));
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
  double doTrial(final int uPerTrial) {
    sketch.reset(); // reuse the same sketch
    final long startUpdateTime_nS = System.nanoTime();

    for (int u = uPerTrial; u-- > 0;) {
      sketch.update(++vIn);
    }
    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return (double) updateTime_nS / uPerTrial;
  }

}
