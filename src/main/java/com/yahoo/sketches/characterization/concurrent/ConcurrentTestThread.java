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
  private long done;

  public ConcurrentTestThread(ConcurrentTestContext context) {
    this.context = context;
    this.done = 0;
  }

  @Override
  public void run() {

    while (!stop.get()) {

      while (done < numOpsToDo.get()) {
        doWork();
        done++;
      }
      context.done(getIndex(), done);
    }
  }

  public void reset() {
    done = 0;
  }

  public void resumeThread(long uPerThread) {
    numOpsToDo.set(uPerThread);
  }

  public void pauseThread() {
    numOpsToDo.set(0);
  }

  public void stopThread() {
    stop.set(true);
  }

  protected abstract void doWork();
  protected abstract int getIndex();


}
