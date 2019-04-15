/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

import com.yahoo.sketches.characterization.AccuracyStats;
import com.yahoo.sketches.characterization.uniquecount.druidhll.HyperLogLogCollector;
import com.yahoo.sketches.characterization.uniquecount.druidhll.HyperLogLogHash;

public class DruidHllAccuracyProfile extends BaseAccuracyProfile {

  private static final HyperLogLogHash hash = HyperLogLogHash.getDefault();
  private static final byte[] bytes = new byte[8]; // for key conversion

  private HyperLogLogCollector sketch;
  private boolean useString;

  @Override
  void configure() {
    final String useStringStr = prop.get("Trials_string");
    useString = (useStringStr == null) ? false : Boolean.parseBoolean(useStringStr);
  }

  @Override
  void doTrial() {
    final int qArrLen = qArr.length;
    sketch = HyperLogLogCollector.makeLatestCollector();
    int lastUniques = 0;
    for (int i = 0; i < qArrLen; i++) {
      final AccuracyStats q = qArr[i];
      final double delta = q.trueValue - lastUniques;
      for (int u = 0; u < delta; u++) {
        if (useString) {
          final String vstr = Long.toHexString(++vIn);
          sketch.add(hash.hash(vstr));
        } else {
          longToByteArray(++vIn, bytes);
          sketch.add(hash.hash(bytes));
        }
      }
      lastUniques += delta;
      final double est = sketch.estimateCardinality();
      q.update(est);
      if (getSize) {
        q.bytes = sketch.toByteArray().length;
      }
    }
  }

  static void longToByteArray(long value, final byte[] bytes) {
    for (int i = 7; i >= 0; i--) {
      bytes[i] = (byte) value;
      value >>>= 8;
    }
  }

}
