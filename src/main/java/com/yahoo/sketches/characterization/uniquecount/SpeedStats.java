/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

/**
 * Holds key metrics from a single speed trial
 *
 * @author Lee Rhodes
 */
public class SpeedStats {
  public double updateTime_nS;

  /**
   * Update
   *
   * @param updateTime_nS the update time for this trial in nanoSeconds.
   */
  public void update(final long updateTime_nS) {
    this.updateTime_nS = updateTime_nS;
  }
}
