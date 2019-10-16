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

package org.apache.datasketches.characterization.theta.concurrent;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.datasketches.characterization.theta.ThetaUpdateSpeedProfile;

/**
 * @author eshcar
 */
public class LockBasedThetaUpdateSpeedProfile extends ThetaUpdateSpeedProfile {
  private ReentrantReadWriteLock lock_ = new ReentrantReadWriteLock();

  @Override
  public double doTrial(final int uPerTrial) {
    sketch.reset(); // reuse the same sketch
    final long startUpdateTime_nS = System.nanoTime();

    for (int u = uPerTrial; u-- > 0;) {
      try {
        lock_.writeLock().lock();
        sketch.update(++vIn);
      } finally {
        lock_.writeLock().unlock();
      }
    }
    final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
    return (double) updateTime_nS / uPerTrial;
  }
}
