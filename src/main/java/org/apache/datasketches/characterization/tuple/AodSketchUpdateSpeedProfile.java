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

import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.characterization.uniquecount.BaseUpdateSpeedProfile;
import org.apache.datasketches.memory.WritableDirectHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUnion;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;

public class AodSketchUpdateSpeedProfile extends BaseUpdateSpeedProfile {
  protected ArrayOfDoublesUpdatableSketch sketch;
  private WritableDirectHandle handle;
  private WritableMemory wmem;


  @Override
  public void configure() {
    //Configure Sketch
    final int lgK = Integer.parseInt(prop.mustGet("LgK"));
    final int numValues = Integer.parseInt(prop.mustGet("NumValues"));
    final float p = Float.parseFloat(prop.mustGet("SamplingProbability"));
    final ResizeFactor rf = ResizeFactor.getRF(Integer.parseInt(prop.mustGet("ResizeFactor")));
    final boolean offheap = Boolean.parseBoolean(prop.mustGet("Offheap"));

    final int k = 1 << lgK;
    final ArrayOfDoublesUpdatableSketchBuilder udBldr = new ArrayOfDoublesUpdatableSketchBuilder()
      .setNominalEntries(k).setNumberOfValues(numValues).setSamplingProbability(p).setResizeFactor(rf);
    if (offheap) {
      final int bytes = ArrayOfDoublesUnion.getMaxBytes(k, numValues);
      handle = WritableMemory.allocateDirect(bytes);
      wmem = handle.get();
      sketch = udBldr.build(wmem);
    } else {
      sketch = udBldr.build();
    }
  }

  @Override
  public void cleanup() {
    if (handle != null) { handle.close(); }
  }

  @Override
  public double doTrial(final int uPerTrial) {
    sketch.reset(); // reuse the same sketch
    double[] values = new double[sketch.getNumValues()];
    final long startUpdateTime_nS = System.nanoTime();

    for (int u = uPerTrial; u-- > 0;) {
      sketch.update(++vIn, values);
    }
    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return (double) updateTime_nS / uPerTrial;
  }

}
