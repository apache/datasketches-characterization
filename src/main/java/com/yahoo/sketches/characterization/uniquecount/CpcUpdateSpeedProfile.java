/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

import com.yahoo.sketches.cpc.CpcSketch;

/**
 * @author Lee Rhodes
 */
public class CpcUpdateSpeedProfile extends BaseUpdateSpeedProfile {
  private CpcSketch sketch;

  @Override
  public void configure() {
    final int lgK = Integer.parseInt(prop.mustGet("LgK"));
    sketch = new CpcSketch(lgK);
  }

  @Override
  public double doTrial(final int uPerTrial) {
    sketch.reset(); // reuse the same sketch
    final long startUpdateTime_nS = System.nanoTime();

    for (int u = uPerTrial; u-- > 0;) {
      sketch.update(++vIn);
    }
    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return (double) updateTime_nS / uPerTrial;
  }

}
