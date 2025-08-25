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
  private long writeOpsCounter;
  private long readOpsCounter;
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
    numWriterThreads = 0;
    numReaderThreads = 0;
    writeOpsCounter = 0;
    readOpsCounter = 0;
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
    writeOpsCounter = 0;
    readOpsCounter = 0;
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
    return writeOpsCounter;
  }

  public long getNumReaderThreads() {
    return readOpsCounter;
  }

  /**
   * Gets the total time in nanoseconds
   * @return total time nanoseconds
   */
  public long getTotalTimeNS() { return totalTimeNS.get(); }

  synchronized void done(final int opType, final long done) {
    if (numDoneWriterThreads == 0) {
      pauseAllThreads();
    }
    if (opType == WRITER_INDEX) {
      writeOpsCounter += done;
      numDoneWriterThreads++;
      if (numDoneWriterThreads == numWriterThreads) {
        //done - take time
        totalTimeNS.set(System.nanoTime() - startTimeNS);
      }
    } else { //reader
      readOpsCounter += done;
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
