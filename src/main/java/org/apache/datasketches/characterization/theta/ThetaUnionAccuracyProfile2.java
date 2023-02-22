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

import org.apache.datasketches.characterization.AccuracyStats;
import org.apache.datasketches.characterization.uniquecount.BaseAccuracyProfile;
import org.apache.datasketches.common.Family;
import org.apache.datasketches.common.ResizeFactor;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;

/**
 * This builds a new sketch with <i>n</i> uniques and directly merges it into a new union,
 * then measures the error of getResult.
 *
 * @author Lee Rhodes
 */
public class ThetaUnionAccuracyProfile2 extends BaseAccuracyProfile {
  private Union union;
  private UpdateSketch sketch;
  private boolean rebuild; //Theta QS Sketch Accuracy

  @Override
  public void configure() {
    final int lgK = Integer.parseInt(prop.mustGet("LgK"));
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
    long lastUniques = 0;
    for (int i = 0; i < qArrLen; i++) {
      final AccuracyStats q = qArr[i];
      final long delta = (long)(q.trueValue - lastUniques);
      sketch.reset(); //reuse the same sketch
      for (long u = 0; u < delta; u++) {
        sketch.update(++vIn);
      }
      if (rebuild) { sketch.rebuild(); } //Resizes down to k. Only useful with QSSketch
      union.union(sketch);
      lastUniques += delta;
      q.update(union.getResult().getEstimate());
    }
  }

}
