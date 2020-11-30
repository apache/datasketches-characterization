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

package org.apache.datasketches.characterization.tuple;

import java.lang.reflect.Array;

import org.apache.datasketches.characterization.uniquecount.BaseUpdateSpeedProfile;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesCompactSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSetOperationBuilder;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUnion;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;

public class AodUnionUpdateSpeedProfile extends BaseUpdateSpeedProfile {
  private int lgK;
  private int numSketches;
  private int numValues;
  private ArrayOfDoublesUpdatableSketch[] updateSketches;
  private ArrayOfDoublesCompactSketch[] compactSketches;
  private ArrayOfDoublesUpdatableSketchBuilder sketchBuilder;
  private ArrayOfDoublesSetOperationBuilder setOpBuilder;

  @Override
  public void configure() {
    lgK = Integer.parseInt(prop.mustGet("LgK"));
    numSketches = Integer.parseInt(prop.mustGet("NumSketches"));
    numValues = Integer.parseInt(prop.mustGet("NumValues"));
    updateSketches = (ArrayOfDoublesUpdatableSketch[]) Array.newInstance(ArrayOfDoublesUpdatableSketch.class, numSketches);
    compactSketches = (ArrayOfDoublesCompactSketch[]) Array.newInstance(ArrayOfDoublesCompactSketch.class, numSketches);
    sketchBuilder = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(1 << lgK).setNumberOfValues(numValues);
    setOpBuilder = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(1 << lgK).setNumberOfValues(numValues);
  }

  @Override
  public double doTrial(final int uPerTrial) {
    for (int i = 0; i < numSketches; i++) {
      updateSketches[i] = sketchBuilder.build();
    }

    double[] values = new double[numValues];
    { // spray keys across all sketches
      int i = 0;
      for (int u = uPerTrial; u-- > 0;) {
        updateSketches[i++].update(++vIn, values);
        if (i == numSketches) { i = 0; }
      }
    }

    { // trim and compact sketches
      for (int i = 0; i < numSketches; i++) {
        updateSketches[i].trim();
        compactSketches[i] = updateSketches[i].compact();
      }
    }

    final ArrayOfDoublesUnion union = setOpBuilder.buildUnion();
    final long startUpdateTime_nS = System.nanoTime();

    for (int i = numSketches; i-- > 0;) {
      union.update(compactSketches[i]);
    }

    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return updateTime_nS;
  }

}
