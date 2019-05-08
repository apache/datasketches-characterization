/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.theta;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.AccuracyStats;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.characterization.uniquecount.BaseAccuracyProfile;
import com.yahoo.sketches.theta.SetOperationBuilder;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.UpdateSketchBuilder;

/**
 * @author Lee Rhodes
 */
public class ThetaUnionAccuracyProfile2 extends BaseAccuracyProfile {
  private Union union;
  private UpdateSketch sketch;
  private boolean rebuild; //Theta QS Sketch Accuracy

  @Override
  public void configure() {
    final Family family = Family.stringToFamily(prop.mustGet("THETA_famName"));
    final float p = Float.parseFloat(prop.mustGet("THETA_p"));
    final ResizeFactor rf = ResizeFactor.getRF(Integer.parseInt(prop.mustGet("THETA_lgRF")));
    final boolean direct = Boolean.parseBoolean(prop.mustGet("THETA_direct"));
    rebuild = Boolean.parseBoolean(prop.mustGet("THETA_rebuild"));
    final int k = 1 << lgK;

    //Configure Union
    final SetOperationBuilder bldr = new SetOperationBuilder();
    bldr.setNominalEntries(k);
    bldr.setP(p);
    bldr.setResizeFactor(rf);
    if (direct) {
      final int bytes = Sketches.getMaxUnionBytes(k);
      final WritableMemory wmem = WritableMemory.allocate(bytes);
      union = bldr.buildUnion(wmem);
    } else {
      union = bldr.buildUnion();
    }

    //Configure Sketch
    final UpdateSketchBuilder bldr2 =  new UpdateSketchBuilder();
    bldr2.setFamily(family);
    bldr2.setP(p);
    bldr2.setResizeFactor(rf);
    bldr2.setNominalEntries(k);
    if (direct) {
      final int bytes = Sketches.getMaxUpdateSketchBytes(k);
      final WritableMemory wmem = WritableMemory.allocate(bytes);
      sketch = bldr2.build(wmem);
    } else {
      sketch = bldr2.build();
    }
  }

  @Override
  public void doTrial() {
    final int qArrLen = qArr.length;
    union.reset(); //reuse the same union
    int lastUniques = 0;
    for (int i = 0; i < qArrLen; i++) {
      final AccuracyStats q = qArr[i];
      final double delta = q.trueValue - lastUniques;
      sketch.reset(); //reuse the same sketch
      for (int u = 0; u < delta; u++) {
        sketch.update(++vIn);
      }
      if (rebuild) { sketch.rebuild(); } //Resizes down to k. Only useful with QSSketch
      union.update(sketch);
      lastUniques += delta;
      q.update(union.getResult().getEstimate());
    }
  }

}
