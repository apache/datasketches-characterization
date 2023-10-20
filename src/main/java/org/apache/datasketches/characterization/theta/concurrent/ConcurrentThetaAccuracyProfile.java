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

package org.apache.datasketches.characterization.theta.concurrent;

import static org.apache.datasketches.thetacommon.ThetaUtil.DEFAULT_UPDATE_SEED;

import org.apache.datasketches.characterization.AccuracyStats;
import org.apache.datasketches.characterization.uniquecount.BaseAccuracyProfile;
import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;

/**
 * @author Lee Rhodes
 */
public class ConcurrentThetaAccuracyProfile extends BaseAccuracyProfile {
  private UpdateSketch sharedSketch;
  private UpdateSketch localSketch;
  private int sharedLgK;
  private int localLgK;
  private int poolThreads;
  private double maxConcurrencyError;
  private boolean ordered;
  private boolean offHeap;
  private boolean rebuild; //Theta QS Sketch Accuracy
  private WritableHandle wdh;
  private WritableMemory wmem;

  @Override
  public void configure() {
    //Configure Sketches
    sharedLgK = Integer.parseInt(prop.mustGet("LgK"));
    localLgK = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_localLgK"));
    poolThreads = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_poolThreads"));
    maxConcurrencyError = Double.parseDouble(prop.mustGet("CONCURRENT_THETA_maxConcurrencyError"));
    ordered = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_ordered"));
    offHeap = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_offHeap"));
    rebuild = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_rebuild"));

    final int maxSharedUpdateBytes = Sketch.getMaxUpdateSketchBytes(1 << sharedLgK);

    if (offHeap) {
      wdh = WritableMemory.allocateDirect(maxSharedUpdateBytes);
      wmem = wdh.getWritable();
    } else {
      wmem = WritableMemory.allocate(maxSharedUpdateBytes);
    }
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    sharedSketch = bldr.buildShared(wmem);
    localSketch = bldr.buildLocal(sharedSketch);
  }

  @Override
  public void doTrial() {
    final int qArrLen = qArr.length;
    //reuse the same sketches
    sharedSketch.reset(); // reset shared sketch first
    localSketch.reset();  // local sketch reset is reading the theta from shared sketch
    long lastUniques = 0;
    for (int i = 0; i < qArrLen; i++) {
      final AccuracyStats q = qArr[i];
      final long delta = (long)(q.trueValue - lastUniques);
      for (long u = 0; u < delta; u++) {
        localSketch.update(++vIn);
      }
      lastUniques += delta;
      if (rebuild) { sharedSketch.rebuild(); } //Resizes down to k. Only useful with QSSketch
      q.update(sharedSketch.getEstimate());
      if (getSize) {
        q.bytes = sharedSketch.compact().toByteArray().length;
      }
    }
  }

  @Override
  public void cleanup() {
    try {
      if (wdh != null) {
        wdh.close();
      }
    } catch (final Exception e) {
      // do nothing
    }
  }

  //configures builder for both local and shared
  UpdateSketchBuilder configureBuilder() {
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setNumPoolThreads(poolThreads);
    bldr.setLogNominalEntries(sharedLgK);
    bldr.setLocalLogNominalEntries(localLgK);
    bldr.setSeed(DEFAULT_UPDATE_SEED);
    bldr.setPropagateOrderedCompact(ordered);
    bldr.setMaxConcurrencyError(maxConcurrencyError);
    bldr.setMaxNumLocalThreads(1);
    return bldr;
  }

}
