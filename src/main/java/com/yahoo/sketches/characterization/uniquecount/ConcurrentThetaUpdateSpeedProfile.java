package com.yahoo.sketches.characterization.uniquecount;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;

import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.theta.ConcurrentSharedThetaSketch;
import com.yahoo.sketches.theta.ConcurrentThetaBuilder;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * @author eshcar
 */
public class ConcurrentThetaUpdateSpeedProfile extends BaseUpdateSpeedProfile {
  private ConcurrentSharedThetaSketch sharedSketch;
  private UpdateSketch localSketch;
  private int sharedLgK;
  private int localLgK;
  private int cacheLimit;
  private boolean ordered;
  private boolean offHeap;
  private boolean sharedIsDirect;
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
    cacheLimit = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_cacheLimit"));
    ordered = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_ordered"));
    offHeap = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_offHeap"));
    sharedIsDirect = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_sharedIsDirect"));

    final int maxSharedUpdateBytes = Sketch.getMaxUpdateSketchBytes(1 << sharedLgK);

    if (offHeap) {
      wdh = WritableMemory.allocateDirect(maxSharedUpdateBytes);
      wmem = wdh.get();
    } else {
      wmem = WritableMemory.allocate(maxSharedUpdateBytes);
    }
    final ConcurrentThetaBuilder bldr = configureBuilder();
    //must build shared first
    sharedSketch = bldr.build(wmem);
    localSketch = bldr.build();

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
    sharedSketch.resetShared(); // reset shared sketch first
    localSketch.reset();  // local sketch reset is reading the theta from shared sketch
    final long startUpdateTime_nS = System.nanoTime();

    for (int u = uPerTrial; u-- > 0;) {
      localSketch.update(++vIn);
    }
    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return (double) updateTime_nS / uPerTrial;
  }

  //configures builder for both local and shared
  ConcurrentThetaBuilder configureBuilder() {
    final ConcurrentThetaBuilder bldr = new ConcurrentThetaBuilder();
    bldr.setSharedLogNominalEntries(sharedLgK);
    bldr.setLocalLogNominalEntries(localLgK);
    bldr.setSeed(DEFAULT_UPDATE_SEED);
    bldr.setCacheLimit(cacheLimit);
    bldr.setPropagateOrderedCompact(ordered);
    bldr.setSharedIsDirect(sharedIsDirect);
    return bldr;
  }

}
