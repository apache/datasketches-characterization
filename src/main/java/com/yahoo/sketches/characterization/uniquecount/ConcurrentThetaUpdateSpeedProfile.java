/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;
import com.yahoo.sketches.theta.UpdateSketchBuilder;

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
  private WritableDirectHandle wdh;
  private WritableMemory wmem;

  /**
   * Configure the sketch
   */
  @Override
  void configure() {
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
      wmem = wdh.get();
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
  double doTrial(final int uPerTrial) {
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
    return bldr;
  }

}
