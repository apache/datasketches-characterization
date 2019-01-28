/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.concurrent;

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
  private long done;

  public ConcurrentTestThread(final ConcurrentTestContext context) {
    this.context = context;
    done = 0;
  }

  @Override
  public void run() {

    while (!stop.get()) {

      while (!paused.get() && (done < numOpsToDo.get())) {
        doWork();
        done++;
      }
      if (resumed.get() && (paused.get() || (done == numOpsToDo.get()))) {
        resumed.set(false);
        context.done(getIndex(), done);
      }
    }
  }

  /**
   * Reset this ConcurrentTestThread
   */
  public void reset() {
    numOpsToDo.set(0);
    done = 0;
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
