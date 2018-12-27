/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */
package com.yahoo.sketches.characterization.concurrent;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A context for starting and stopping multiple concurrent threads running a test
 *
 * @author eshcar
 */
public class ConcurrentTestContext {

  private final int numThreadTypes;
  private List<Set<ConcurrentTestThread>> testThreadsList;
  private List<Long> perThreadTypeCounter;
  private int numThreads = 0;
  private int doneThreads = 0;

  private long startTimeNS;
  private AtomicLong totalTimeNS = new AtomicLong();

 public ConcurrentTestContext(int numThreadTypes) {
    this.numThreadTypes = numThreadTypes;
    testThreadsList = new ArrayList<>(numThreadTypes);
    for (int i = 0; i < numThreadTypes; i++) {
      testThreadsList.add(new HashSet<>());
    }
    perThreadTypeCounter = new ArrayList<>(numThreadTypes);
    for (int i = 0; i < numThreadTypes; i++) {
      perThreadTypeCounter.add(0L);
    }
  }

  public synchronized void addThread(int index, ConcurrentTestThread t) {
    testThreadsList.get(index).add(t);
    numThreads++;
  }

  public void startThreads() {
    for (int i = 0; i < numThreadTypes; i++) {
      for (ConcurrentTestThread t : testThreadsList.get(i)) {
        t.start();
      }
    }
  }

  public void reset() {
    for (int i = 0; i < numThreadTypes; i++) {
      for (ConcurrentTestThread t : testThreadsList.get(i)) {
        t.reset();
      }
    }
    for (int i = 0; i < numThreadTypes; i++) {
      perThreadTypeCounter.set(i, 0L);
    }
    doneThreads = 0;
    totalTimeNS.set(0);

  }

  public void doTrial(long uPerTrial) {
    long uPerThread = uPerTrial / numThreads;
    uPerThread = Math.max(uPerThread, 1);
    //start counting time
    startTimeNS = System.nanoTime();

    for (int i = 0; i < numThreadTypes; i++) {
      for (ConcurrentTestThread t : testThreadsList.get(i)) {
        t.resumeThread(uPerThread);
      }
    }
  }

  public void waitForAll() {
    while (totalTimeNS.get() == 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public long getTotalTimeNS() { return totalTimeNS.get(); }
  public long getThreadTypeCounter(int index) {
    return perThreadTypeCounter.get(index);
  }


  synchronized void done(int index, long done) {
    if(doneThreads == 0) {
      pauseAllThreads();
    }
    long l = perThreadTypeCounter.get(index);
    perThreadTypeCounter.set(index, l+done);
    doneThreads++;
    if(doneThreads == numThreads) {
      //done - take time time
      totalTimeNS.set(System.nanoTime() - startTimeNS);
    }

  }

  private void pauseAllThreads() {
    for (int i = 0; i < numThreadTypes; i++) {
      for (ConcurrentTestThread t : testThreadsList.get(i)) {
        t.pauseThread();
      }
    }
  }

}
