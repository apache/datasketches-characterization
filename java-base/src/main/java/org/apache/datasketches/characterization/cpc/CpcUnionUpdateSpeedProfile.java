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

import java.lang.reflect.Array;

import org.apache.datasketches.characterization.uniquecount.BaseUpdateSpeedProfile;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;

public class CpcUnionUpdateSpeedProfile extends BaseUpdateSpeedProfile {
  private int lgK;
  private int numSketches;
  private CpcSketch[] sketches;

  @Override
  public void configure() {
    lgK = Integer.parseInt(prop.mustGet("LgK"));
    numSketches = Integer.parseInt(prop.mustGet("NumSketches"));
    sketches = (CpcSketch[]) Array.newInstance(CpcSketch.class, numSketches);
  }

  @Override
  public double doTrial(final int uPerTrial) {
    for (int i = 0; i < numSketches; i++) {
      sketches[i] = new CpcSketch(lgK);
    }

    { // spray values across all sketches
      int i = 0;
      for (int u = uPerTrial; u-- > 0;) {
        sketches[i++].update(++vIn);
        if (i == numSketches) { i = 0; }
      }
    }

    final CpcUnion union = new CpcUnion(lgK);
    final long startUpdateTime_nS = System.nanoTime();

    for (int i = numSketches; i-- > 0;) {
      union.update(sketches[i]);
    }

    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return updateTime_nS;
  }

}
