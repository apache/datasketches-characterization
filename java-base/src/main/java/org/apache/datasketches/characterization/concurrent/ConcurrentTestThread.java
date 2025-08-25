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

package org.apache.datasketches.characterization.concurrent;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An abstract concurrent worker thread, to carry any work for a given number of times or until stopped by
 * the context of the test.
 *
 * @author eshcar
 */
public abstract class ConcurrentTestThread extends Thread {

  private ConcurrentTestContext context;
  private AtomicBoolean stop = new AtomicBoolean(false);
  private AtomicLong numOpsToDo = new AtomicLong(0);
  private AtomicBoolean resumed = new AtomicBoolean(false);
  private AtomicBoolean paused = new AtomicBoolean(false);
  private long opCount;

  public ConcurrentTestThread(final ConcurrentTestContext context) {
    this.context = context;
    opCount = 0;
  }

  @Override
  public void run() {

    while (!stop.get()) {

      while (!paused.get() && (opCount < numOpsToDo.get())) {
        doWork();
        opCount++;
      }
      if (resumed.get() && (paused.get() || (opCount == numOpsToDo.get()))) {
        resumed.set(false);
        context.done(getIndex(), opCount);
      }
    }
  }

  /**
   * Reset this ConcurrentTestThread
   */
  public void reset() {
    numOpsToDo.set(0);
    opCount = 0;
    paused.set(false);
  }

  public void resumeThread(final long uPerThread) {
    numOpsToDo.set(uPerThread);
    resumed.set(true);
  }

  public void pauseThread() {
    paused.set(true);
  }

  public void stopThread() {
    stop.set(true);
  }

  protected abstract void doWork();

  protected abstract int getIndex();

}
