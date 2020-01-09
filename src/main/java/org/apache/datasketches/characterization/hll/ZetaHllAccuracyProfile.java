/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.characterization.hll;

import org.apache.datasketches.AccuracyStats;
import org.apache.datasketches.characterization.uniquecount.BaseAccuracyProfile;

import com.google.zetasketch.HyperLogLogPlusPlus;

/**
 * @author Lee Rhodes
 */
public class ZetaHllAccuracyProfile extends BaseAccuracyProfile {
  private enum ZetaType { LONG, INTEGER, STRING, BYTES }

  private HyperLogLogPlusPlus<?> sketch;
  private HyperLogLogPlusPlus.Builder hllBuilder;
  private int lgSP;
  private String zetaType;
  private ZetaType zType;

  @Override
  public void configure() {
    lgSP = Integer.parseInt(prop.mustGet("LgSP"));
    zetaType = prop.mustGet("ZetaType");
    hllBuilder = new HyperLogLogPlusPlus.Builder();
    hllBuilder.normalPrecision(lgK);
    hllBuilder.sparsePrecision(lgSP);
    if (zetaType.equals("LONG")) {
      zType = ZetaType.LONG;
    } else if (zetaType.equals("INTEGER")) {
      zType = ZetaType.INTEGER;
    } else if (zetaType.equals("STRING")) {
      zType = ZetaType.STRING;
    } else if (zetaType.equals("BYTES")) {
      zType = ZetaType.BYTES;
    }
    reset();
  }

  private void reset() {
    switch (zType) {
      case LONG:    sketch = hllBuilder.buildForLongs(); break;
      case INTEGER: sketch = hllBuilder.buildForIntegers(); break;
      case STRING:  sketch = hllBuilder.buildForStrings(); break;
      case BYTES:   sketch = hllBuilder.buildForBytes(); break;
    }
  }

  @Override
  public void doTrial() {
    final int qArrLen = qArr.length;
    reset();
    long lastUniques = 0;
    for (int i = 0; i < qArrLen; i++) {
      final AccuracyStats q = qArr[i];
      final long delta = (long)(q.trueValue - lastUniques);
      for (long u = 0; u < delta; u++) {
        sketch.add(++vIn);
      }
      lastUniques += delta;
      final double est = sketch.result();
      q.update(est);
    }
  }

}
