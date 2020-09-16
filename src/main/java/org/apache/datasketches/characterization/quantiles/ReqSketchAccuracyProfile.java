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

package org.apache.datasketches.characterization.quantiles;

import org.apache.datasketches.Properties;
import org.apache.datasketches.req.Criteria;
import org.apache.datasketches.req.ReqSketch;

/**
 * @author Lee Rhodes
 */
public class ReqSketchAccuracyProfile extends BaseReqSketchAccuracyProfile {
  private ReqSketch sk;
  @SuppressWarnings("unused")
  private boolean useBulk;


  @Override
  void configure(final Properties props) {
    final int K = Integer.parseInt(job.getProperties().mustGet("K"));
    final Criteria criterion = Criteria.valueOf(job.getProperties().mustGet("criterion"));
    final boolean hra = Boolean.parseBoolean(job.getProperties().mustGet("hra"));
    useBulk = Boolean.parseBoolean(job.getProperties().mustGet("useBulk"));
    sk = new ReqSketch(K, hra);
    sk.setCriterion(criterion);
  }

  @Override
  void prepareTrial() {  }

  @Override
  double[] doTrial() {
    final int slen = stream.length;
    for (int i = 0; i < slen; i++) {
      sk.update(stream[i]);
    }

    final double[] rawRanks = sk.getRanks(evenlySpacedValues);
    final double[] re = new double[numEvenlySpaced];
    //compute error
    for (int i = 1; i < numEvenlySpaced; i++) {
      re[i] = rawRanks[i] - evenlySpacedRanks[i];
    }
    sk.reset();
    return re;
  }

}
