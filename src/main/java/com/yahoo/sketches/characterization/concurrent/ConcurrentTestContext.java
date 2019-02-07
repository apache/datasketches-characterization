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

  public static final int WRITER_INDEX = 0;
  public static final int READER_INDEX = 1;

  private static final int NUM_THREAD_TYPES = 2;
  private List<Set<ConcurrentTestThread>> testThreadsList;
  private List<Long> perThreadTypeCounter;
  private int numWriteThreads = 0;
  private int numReaderThreads = 0;
  private volatile int[] doneThreads;

  private long startTimeNS;
  private AtomicLong totalTimeNS = new AtomicLong();

  /**
   * Construct a ConcurrentTestContext
   */
  public ConcurrentTestContext() {
    testThreadsList = new ArrayList<>(NUM_THREAD_TYPES);
    for (int i = 0; i < NUM_THREAD_TYPES; i++) {
      testThreadsList.add(new HashSet<>());
    }
    perThreadTypeCounter = new ArrayList<>(NUM_THREAD_TYPES);
    for (int i = 0; i < NUM_THREAD_TYPES; i++) {
      perThreadTypeCounter.add(0L);
    }
    doneThreads = new int[]{0, 0};
  }

  public synchronized void addWriterThread(final ConcurrentTestThread t) {
    testThreadsList.get(WRITER_INDEX).add(t);
    numWriteThreads++;
  }

  public synchronized void addReaderThread(final ConcurrentTestThread t) {
    testThreadsList.get(READER_INDEX).add(t);
    numReaderThreads++;
  }

  /**
   * Start all threads
   */
  public void startThreads() {
    for (int i = 0; i < NUM_THREAD_TYPES; i++) {
      for (ConcurrentTestThread t : testThreadsList.get(i)) {
        t.start();
      }
    }
  }

  /**
   * Reset this context
   */
  public synchronized void reset() {
    for (int i = 0; i < NUM_THREAD_TYPES; i++) {
      for (ConcurrentTestThread t : testThreadsList.get(i)) {
        t.reset();
      }
    }
    for (int i = 0; i < NUM_THREAD_TYPES; i++) {
      perThreadTypeCounter.set(i, 0L);
    }
    doneThreads[WRITER_INDEX] = 0;
    doneThreads[READER_INDEX] = 0;
    totalTimeNS.set(0);

  }

  /**
   * Do a single trial
   * @param uPerTrial number of uniques per trial
   */
  public void doTrial(final long uPerTrial) {
    long uPerThread = uPerTrial / numWriteThreads;
    uPerThread = Math.max(uPerThread, 1);
    //start counting time
    startTimeNS = System.nanoTime();

    for (ConcurrentTestThread t : testThreadsList.get(WRITER_INDEX)) {
      t.resumeThread(uPerThread);
    }
    for (ConcurrentTestThread t : testThreadsList.get(READER_INDEX)) {
      t.resumeThread(Long.MAX_VALUE);
    }
  }

  /**
   * Wait for all threads
   */
  public void waitForAll() {
    while ((doneThreads[WRITER_INDEX] < numWriteThreads) || (doneThreads[READER_INDEX] < numReaderThreads)) {
      try {
        Thread.sleep(1);
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public long getNumWrites() {
    return getThreadTypeCounter(WRITER_INDEX);
  }

  /**
   * Gets the total time in nanoseconds
   * @return total time nanoseconds
   */
  public long getTotalTimeNS() { return totalTimeNS.get(); }

  /**
   * Gets the Thread-type counter
   * @param index the counter index
   */
  private long getThreadTypeCounter(final int index) {
    return perThreadTypeCounter.get(index);
  }


  synchronized void done(final int index, final long done) {
    if (doneThreads[WRITER_INDEX] == 0) {
      pauseAllThreads();
    }
    final long l = perThreadTypeCounter.get(index);
    perThreadTypeCounter.set(index, l + done);
    doneThreads[index]++;
    if (doneThreads[WRITER_INDEX] == numWriteThreads) {
      //done - take time
      totalTimeNS.set(System.nanoTime() - startTimeNS);
    }

  }

  private void pauseAllThreads() {
    for (int i = 0; i < NUM_THREAD_TYPES; i++) {
      for (ConcurrentTestThread t : testThreadsList.get(i)) {
        if (!t.equals(Thread.currentThread())) {
          t.pauseThread();
        }
      }
    }
  }

  /**
   * Stop all threads
   */
  public void stopAllThreads() {
    for (int i = 0; i < NUM_THREAD_TYPES; i++) {
      for (ConcurrentTestThread t : testThreadsList.get(i)) {
        t.stopThread();
      }
    }
  }

}