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

import org.apache.datasketches.characterization.uniquecount.BaseSerDeProfile;

import com.google.zetasketch.HyperLogLogPlusPlus;

/**
 * @author Lee Rhodes
 */
public class ZetaHllSerDeProfile extends BaseSerDeProfile {
  private enum ZetaType { LONG, INTEGER, STRING, BYTES }

  private HyperLogLogPlusPlus<?> sketch1;
  private HyperLogLogPlusPlus<?> sketch2;
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
      case LONG:
        sketch1 = hllBuilder.buildForLongs();
        sketch2 = hllBuilder.buildForLongs();
        break;
      case INTEGER:
        sketch1 = hllBuilder.buildForIntegers();
        sketch2 = hllBuilder.buildForIntegers();
        break;
      case STRING:
        sketch1 = hllBuilder.buildForStrings();
        sketch2 = hllBuilder.buildForStrings();
        break;
      case BYTES:
        sketch1 = hllBuilder.buildForBytes();
        sketch2 = hllBuilder.buildForBytes();
        break;
    }
  }

  @Override
  public void doTrial(final long[] stats, final int uPerTrial) {
    reset();

    for (int u = uPerTrial; u-- > 0;) {
      sketch1.add(++vIn);
    }

    final long startEstTime_nS = System.nanoTime();
    final double est1 = sketch1.result();

    final long startSerTime_nS = System.nanoTime();
    final byte[] byteArr = sketch1.serializeToByteArray();

    final long startDeSerTime_nS = System.nanoTime();
    sketch2 = HyperLogLogPlusPlus.forProto(byteArr);
    final long endTime_nS = System.nanoTime();

    final double est2 = sketch2.result();
    assert est1 == est2;

    stats[est_ns] = startSerTime_nS - startEstTime_nS;
    stats[ser_ns] = startDeSerTime_nS - startSerTime_nS;
    stats[deser_ns] = endTime_nS - startDeSerTime_nS;
    stats[size_bytes] = byteArr.length;
  }

}
