/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.hll.HyperLogLogHash;

public class DruidHllAccuracyProfile extends BaseAccuracyProfile {

  private static final HyperLogLogHash hash = HyperLogLogHash.getDefault();
  private static final byte[] bytes = new byte[8]; // for key conversion

  private HyperLogLogCollector sketch;
  private boolean useString;

  @Override
  public void configure() {
    final String useStringStr = prop.get("Trials_string");
    useString = (useStringStr == null) ? false : Boolean.parseBoolean(useStringStr);
  }

  @Override
  public void doTrial() {
    final int qArrLen = qArr.length;
    sketch = HyperLogLogCollector.makeLatestCollector();
    long lastUniques = 0;
    for (int i = 0; i < qArrLen; i++) {
      final AccuracyStats q = qArr[i];
      final long delta = (long)(q.trueValue - lastUniques);
      for (long u = 0; u < delta; u++) {
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
