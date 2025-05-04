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

import java.lang.reflect.Array;

import org.apache.datasketches.characterization.uniquecount.BaseUpdateSpeedProfile;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;

public class ThetaUnionUpdateSpeedProfile extends BaseUpdateSpeedProfile {
  private int lgK;
  private int numSketches;
  private UpdateSketch[] updateSketches;
  private CompactSketch[] compactSketches;
  private UpdateSketchBuilder sketchBuilder;
  private SetOperationBuilder setOpBuilder;

  @Override
  public void configure() {
    lgK = Integer.parseInt(prop.mustGet("LgK"));
    numSketches = Integer.parseInt(prop.mustGet("NumSketches"));
    updateSketches = (UpdateSketch[]) Array.newInstance(UpdateSketch.class, numSketches);
    compactSketches = (CompactSketch[]) Array.newInstance(CompactSketch.class, numSketches);
    sketchBuilder = Sketches.updateSketchBuilder().setNominalEntries(1 << lgK);
    setOpBuilder = Sketches.setOperationBuilder().setNominalEntries(1 << lgK);
  }

  @Override
  public double doTrial(final int uPerTrial) {
    for (int i = 0; i < numSketches; i++) {
      updateSketches[i] = sketchBuilder.build();
    }

    { // spray values across all sketches
      int i = 0;
      for (int u = uPerTrial; u-- > 0;) {
        updateSketches[i++].update(++vIn);
        if (i == numSketches) { i = 0; }
      }
    }

    { // trim and compact sketches
      for (int i = 0; i < numSketches; i++) {
        compactSketches[i] = updateSketches[i].rebuild().compact();
      }
    }

    final Union union = setOpBuilder.buildUnion();
    final long startUpdateTime_nS = System.nanoTime();

    for (int i = numSketches; i-- > 0;) {
      union.union(compactSketches[i]);
    }

    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return updateTime_nS;
  }

}
