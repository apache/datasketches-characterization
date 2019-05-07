/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.cpc;

import com.yahoo.sketches.characterization.uniquecount.BaseSerDeProfile;
import com.yahoo.sketches.cpc.CpcSketch;

/**
 * @author Lee Rhodes
 */
public class CpcSerDeProfile extends BaseSerDeProfile {
  private CpcSketch sketch;

  @Override
  public void configure() {
    sketch = new CpcSketch(lgK);
  }


  @Override
  public void doTrial(final Stats stats, final int uPerTrial) {
    sketch.reset(); // reuse the same sketch

    for (int u = uPerTrial; u-- > 0;) {
      sketch.update(++vIn);
    }
    final double est1 = sketch.getEstimate();

    final byte[] byteArr;
    final long startSerTime_nS, stopSerTime_nS;

    startSerTime_nS = System.nanoTime();
    byteArr = sketch.toByteArray();
    stopSerTime_nS = System.nanoTime();

    final long startDeserTime_nS, stopDeserTime_nS;

    startDeserTime_nS = System.nanoTime();
    final CpcSketch sketch2 = CpcSketch.heapify(byteArr);
    stopDeserTime_nS = System.nanoTime();

    final double est2 = sketch2.getEstimate();
    assert est1 == est2;

    stats.serializeTime_nS = stopSerTime_nS - startSerTime_nS;
    stats.deserializeTime_nS = stopDeserTime_nS - startDeserTime_nS;
  }

}
