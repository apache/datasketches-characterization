/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.theta;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.AccuracyStats;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.characterization.uniquecount.BaseAccuracyProfile;
import com.yahoo.sketches.theta.SetOperationBuilder;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;

/**
 * @author Lee Rhodes
 */
public class ThetaUnionAccuracyProfile extends BaseAccuracyProfile {
  private Union union;

  @Override
  public void configure() {
    //Configure Sketch
    final float p = Float.parseFloat(prop.mustGet("THETA_p"));
    final ResizeFactor rf = ResizeFactor.getRF(Integer.parseInt(prop.mustGet("THETA_lgRF")));
    final boolean direct = Boolean.parseBoolean(prop.mustGet("THETA_direct"));

    final int k = 1 << lgK;
    final SetOperationBuilder bldr = new SetOperationBuilder();
    bldr.setNominalEntries(k);
    bldr.setP(p);
    bldr.setResizeFactor(rf);
    if (direct) {
      final int bytes = Sketch.getMaxUpdateSketchBytes(k);
      final WritableMemory wmem = WritableMemory.allocate(bytes);
      union = bldr.buildUnion(wmem);
    } else {
      union = bldr.buildUnion();
    }
  }

  @Override
  public void doTrial() {
    final int qArrLen = qArr.length;
    union.reset(); //reuse the same sketch
    int lastUniques = 0;
    for (int i = 0; i < qArrLen; i++) {
      final AccuracyStats q = qArr[i];
      final double delta = q.trueValue - lastUniques;
      for (int u = 0; u < delta; u++) {
        union.update(++vIn);
      }
      lastUniques += delta;
      q.update(union.getResult().getEstimate());
    }
  }

}
