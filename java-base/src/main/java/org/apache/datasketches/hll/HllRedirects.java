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

package org.apache.datasketches.hll;

import static org.apache.datasketches.hll.HllEstimators.hllCompositeEstimate;
import static org.apache.datasketches.hll.HllEstimators.hllLowerBound;
import static org.apache.datasketches.hll.HllEstimators.hllUpperBound;
import static org.apache.datasketches.hll.RelativeErrorTables.getRelErr;

/**
 * Provide access to hidden HLL methods.
 *
 * @author Lee Rhodes
 */
public class HllRedirects {

  public double getRelativeError(final boolean upperBound, final boolean oooFlag,
      final int lgK, final int stdDev) {
    return getRelErr(upperBound, oooFlag, lgK, stdDev);
  }

  public double getHllLowerBound(final HllSketch sk, final int numStdDev) {
    return hllLowerBound((AbstractHllArray)sk.hllSketchImpl, numStdDev);
  }

  public double getHllUpperBound(final HllSketch sk, final int numStdDev) {
    return hllUpperBound((AbstractHllArray)sk.hllSketchImpl, numStdDev);
  }

  public double getHIPEstimate(final HllSketch sk) {
    return getAbstractHllArray(sk).getHipEstimate();
  }

  public double getCompositeEstimate(final HllSketch sk) {
    return hllCompositeEstimate((AbstractHllArray)sk.hllSketchImpl);
  }

  public AbstractHllArray getAbstractHllArray(final HllSketch sk) {
    return (AbstractHllArray)sk.hllSketchImpl;
  }

}
