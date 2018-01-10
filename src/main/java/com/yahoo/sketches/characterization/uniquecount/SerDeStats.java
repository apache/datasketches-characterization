/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

/**
 * Holds key metrics from a single SerDe trial
 *
 * @author Lee Rhodes
 */
public class SerDeStats {
  public long serializeTime_nS;
  public long deserializeTime_nS;


  /**
   * Update
   *
   * @param ser_nS the serialization time for this trial in nanoseconds.
   * @param deser_nS the deserialization time for this trial in nanoseconds.
   */
  public void update(final long ser_nS, final long deser_nS) {
    serializeTime_nS = ser_nS;
    deserializeTime_nS = deser_nS;
  }
}
