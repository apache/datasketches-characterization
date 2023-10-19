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

import org.apache.datasketches.characterization.uniquecount.BaseUpdateSpeedProfile;
import org.apache.datasketches.memory.WritableHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;

/**
 * @author eshcar
 */
public class ConcurrentThetaUpdateSpeedProfile extends BaseUpdateSpeedProfile {
  private UpdateSketch sharedSketch;
  private UpdateSketch localSketch;
  private int sharedLgK;
  private int localLgK;
  private boolean ordered;
  private boolean offHeap;
  private int poolThreads;
  private double maxConcurrencyError;
  private WritableHandle wdh;
  private WritableMemory wmem;

  /**
   * Configure the sketch
   */
  @Override
  public void configure() {
    //Configure Sketches
    sharedLgK = Integer.parseInt(prop.mustGet("LgK"));
    localLgK = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_localLgK"));
    poolThreads = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_poolThreads"));
    maxConcurrencyError = Double.parseDouble(prop.mustGet("CONCURRENT_THETA_maxConcurrencyError"));
    ordered = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_ordered"));
    offHeap = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_offHeap"));

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

  /**
   * Return the average update time per update for this trial
   *
   * @param uPerTrial the number of unique updates for this trial
   * @return the average update time per update for this trial
   */
  @Override
  public double doTrial(final int uPerTrial) {
    //reuse the same sketches
    sharedSketch.reset(); // reset shared sketch first
    localSketch.reset();  // local sketch reset is reading the theta from shared sketch
    final long startUpdateTime_nS = System.nanoTime();

    for (int u = uPerTrial; u-- > 0;) {
      localSketch.update(++vIn);
    }
    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return (double) updateTime_nS / uPerTrial;
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
