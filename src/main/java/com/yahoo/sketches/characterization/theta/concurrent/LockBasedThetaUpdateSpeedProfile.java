package com.yahoo.sketches.characterization.theta.concurrent;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.yahoo.sketches.characterization.theta.ThetaUpdateSpeedProfile;

/**
 * @author eshcar
 */
public class LockBasedThetaUpdateSpeedProfile extends ThetaUpdateSpeedProfile {
  private ReentrantReadWriteLock lock_ = new ReentrantReadWriteLock();

  @Override
  public double doTrial(final int uPerTrial) {
    sketch.reset(); // reuse the same sketch
    final long startUpdateTime_nS = System.nanoTime();

    for (int u = uPerTrial; u-- > 0;) {
      try {
        lock_.writeLock().lock();
        sketch.update(++vIn);
      } finally {
        lock_.writeLock().unlock();
      }
    }
    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return (double) updateTime_nS / uPerTrial;
  }
}
