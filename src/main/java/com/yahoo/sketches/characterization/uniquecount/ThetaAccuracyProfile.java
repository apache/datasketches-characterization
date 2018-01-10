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
public class ThetaAccuracyProfile extends BaseAccuracyProfile {
  private UpdateSketch sketch;
  private boolean rebuild; //Theta QS Sketch Accuracy

  @Override
  void configure() {
    //Configure Sketch
    final Family family = Family.stringToFamily(prop.mustGet("THETA_famName"));
    final float p = Float.parseFloat(prop.mustGet("THETA_p"));
    final ResizeFactor rf = ResizeFactor.getRF(Integer.parseInt(prop.mustGet("THETA_lgRF")));
    final boolean direct = Boolean.parseBoolean(prop.mustGet("THETA_direct"));
    rebuild = Boolean.parseBoolean(prop.mustGet("THETA_rebuild"));

    final int k = 1 << lgK;
    final UpdateSketchBuilder udBldr = UpdateSketch.builder()
        .setNominalEntries(k)
        .setFamily(family)
        .setP(p)
        .setResizeFactor(rf);
    if (direct) {
      final int bytes = Sketch.getMaxUpdateSketchBytes(k);
      final WritableMemory wmem = WritableMemory.allocate(bytes);
      sketch = udBldr.build(wmem);
    } else {
      sketch = udBldr.build();
    }
  }

  @Override
  void doTrial() {
    final int qArrLen = qArr.length;
    sketch.reset(); //reuse the same sketch
    int lastUniques = 0;
    for (int i = 0; i < qArrLen; i++) {
      final AccuracyStats q = qArr[i];
      final double delta = q.trueValue - lastUniques;
      for (int u = 0; u < delta; u++) {
        sketch.update(++vIn);
      }
      lastUniques += delta;
      if (rebuild) { sketch.rebuild(); } //Resizes down to k. Only useful with QSSketch
      q.update(sketch.getEstimate());
      if (getSize) {
        q.bytes = sketch.compact().toByteArray().length;
      }
    }
  }

}
