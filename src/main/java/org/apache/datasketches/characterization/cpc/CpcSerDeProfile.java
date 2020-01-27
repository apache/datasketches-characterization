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

package org.apache.datasketches.characterization.cpc;

import org.apache.datasketches.characterization.uniquecount.BaseSerDeProfile;
import org.apache.datasketches.cpc.CpcSketch;

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
    stats.size_bytes = byteArr.length;
  }

}
