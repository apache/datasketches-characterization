/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.concurrent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A context for starting and stopping multiple concurrent threads running a test
 *
 * @author eshcar
 * @author Lee Rhodes
 */
public class ConcurrentTestContext {

  public static final int WRITER_INDEX = 0;
  public static final int READER_INDEX = 1;

  private Set<ConcurrentTestThread> writerThreadsSet;
  private Set<ConcurrentTestThread> readerThreadsSet;
  private long writerThreadCounter;
  private long readerThreadCounter;
  private int numWriterThreads;
  private int numReaderThreads;
  private int numDoneWriterThreads;
  private int numDoneReaderThreads;

  private long startTimeNS;
  private AtomicLong totalTimeNS = new AtomicLong();

  /**
   * Construct a ConcurrentTestContext
   */
  public ConcurrentTestContext() {
    writerThreadsSet = new HashSet<>();
    readerThreadsSet = new HashSet<>();
    writerThreadCounter = 0;
    readerThreadCounter = 0;
    numDoneWriterThreads = 0;
    numDoneReaderThreads = 0;
  }

  public synchronized void addWriterThread(final ConcurrentTestThread t) {
    writerThreadsSet.add(t);
    numWriterThreads++;
  }

  public synchronized void addReaderThread(final ConcurrentTestThread t) {
    readerThreadsSet.add(t);
    numReaderThreads++;
  }

  /**
   * Start all threads
   */
  public void startThreads() {
    for (ConcurrentTestThread t : writerThreadsSet) {
      t.start();
    }
    for (ConcurrentTestThread t : readerThreadsSet) {
      t.start();
    }
  }

  /**
   * Reset this context
   */
  public synchronized void reset() {
    for (ConcurrentTestThread t : writerThreadsSet) {
      t.reset();
    }
    for (ConcurrentTestThread t : readerThreadsSet) {
      t.reset();
    }
    writerThreadCounter = 0;
    readerThreadCounter = 0;
    numDoneWriterThreads = 0;
    numDoneReaderThreads = 0;
    totalTimeNS.set(0);
  }

  /**
   * Do a single trial
   * @param uPerTrial number of uniques per trial
   */
  public void doTrial(final long uPerTrial) {
    long uPerThread = uPerTrial / numWriterThreads;
    uPerThread = Math.max(uPerThread, 1);
    //start counting time
    startTimeNS = System.nanoTime();

    for (ConcurrentTestThread t : writerThreadsSet) {
      t.resumeThread(uPerThread);
    }
    for (ConcurrentTestThread t : readerThreadsSet) {
      t.resumeThread(Long.MAX_VALUE);
    }
  }

  /**
   * Wait for all threads
   */
  public void waitForAll() {
    while ((numDoneWriterThreads < numWriterThreads) || (numDoneReaderThreads < numReaderThreads)) {
      try {
        Thread.sleep(1);
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public long getNumWriterThreads() {
    return writerThreadCounter;
  }

  public long getNumReaderThreads() {
    return readerThreadCounter;
  }

  /**
   * Gets the total time in nanoseconds
   * @return total time nanoseconds
   */
  public long getTotalTimeNS() { return totalTimeNS.get(); }

  synchronized void done(final int index, final long done) {
    if (numDoneWriterThreads == 0) {
      pauseAllThreads();
    }
    if (index == WRITER_INDEX) {
      writerThreadCounter += done; //TODO WHY?
      numDoneWriterThreads++;
      if (numDoneWriterThreads == numWriterThreads) {
        //done - take time
        totalTimeNS.set(System.nanoTime() - startTimeNS);
      }
    } else { //reader
      readerThreadCounter += done;  //TODO WHY?
      numDoneReaderThreads++;
    }
  }

  private void pauseAllThreads() {
    for (ConcurrentTestThread t : writerThreadsSet) {
      if (!t.equals(Thread.currentThread())) {
        t.pauseThread();
      }
    }
    for (ConcurrentTestThread t : readerThreadsSet) {
      if (!t.equals(Thread.currentThread())) {
        t.pauseThread();
      }
    }
  }

  /**
   * Stop all threads
   */
  public void stopAllThreads() {
    for (ConcurrentTestThread t : writerThreadsSet) {
      t.stopThread();
    }
    for (ConcurrentTestThread t : readerThreadsSet) {
      t.stopThread();
    }
  }

}
