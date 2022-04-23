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

import static org.apache.datasketches.Util.DEFAULT_UPDATE_SEED;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.datasketches.characterization.concurrent.ConcurrentTestContext;
import org.apache.datasketches.characterization.concurrent.ConcurrentTestThread;
import org.apache.datasketches.characterization.uniquecount.BaseUpdateSpeedProfile;
import org.apache.datasketches.memory.WritableDirectHandle;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;

/**
 * Test scalability of concurrent theta sketch - test with multiple writers/readers
 *
 * @author eshcar
 */
public class ConcurrentThetaMultithreadedSpeedProfile extends BaseUpdateSpeedProfile {

  private UpdateSketch sharedSketch;
  private ReentrantReadWriteLock lock;
  private int sharedLgK;
  private int localLgK;
  private boolean ordered;
  private boolean offHeap;
  private int poolThreads;
  private double maxConcurrencyError;
  private WritableDirectHandle wdh;
  private WritableMemory wmem;

  private int numWriterThreads;
  private int numReaderThreads;
  private double writesRatio;
  private boolean isThreadSafe;
  private ConcurrentTestContext ctx;

  /**
   * Configure the sketch
   */
  @Override
  public void configure() {
    //Configure Sketches
    sharedLgK = Integer.parseInt(prop.mustGet("LgK"));
    localLgK = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_localLgK"));
    ordered = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_ordered"));
    offHeap = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_offHeap"));
    poolThreads = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_poolThreads"));
    maxConcurrencyError = Double.parseDouble(prop.mustGet("CONCURRENT_THETA_maxConcurrencyError"));
    numReaderThreads = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_numReaders"));
    numWriterThreads = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_numWriters"));
    writesRatio = Double.parseDouble(prop.mustGet("CONCURRENT_THETA_writersRatio"));
    isThreadSafe = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_ThreadSafe"));

    final int maxSharedUpdateBytes = Sketch.getMaxUpdateSketchBytes(1 << sharedLgK);

    if (offHeap) {
      wdh = WritableMemory.allocateDirect(maxSharedUpdateBytes);
      wmem = wdh.get();
    } else {
      wmem = null; //WritableMemory.allocate(maxSharedUpdateBytes);
    }
    final UpdateSketchBuilder bldr = configureBuilder();
    //must build shared first
    sharedSketch = bldr.buildShared(wmem);
    if (!isThreadSafe) {
      sharedSketch = bldr.build();
      lock = new ReentrantReadWriteLock();
    }
    ctx = new ConcurrentTestContext();
    for (int i = 0; i < numWriterThreads; i++) {
      final WriterThread writer;
      if (writesRatio != 0) {
        if (isThreadSafe) {
          writer = new ReaderWriterThread(bldr, i, writesRatio);
        } else {
          writer = new LockBasedReaderWriterThread(i, writesRatio);
        }
      } else {
        if (isThreadSafe) {
          writer = new WriterThread(bldr, i);
        } else {
          writer = new LockBasedWriterThread(i);
        }
      }
      ctx.addWriterThread(writer);
    }
    for (int i = 0; i < numReaderThreads; i++) {
      final ReaderThread reader;
      if (isThreadSafe) {
        reader  = new BackgroundReaderThread();
      } else {
        reader = new LockBasedBackgroundReaderThread();
      }
      ctx.addReaderThread(reader);
    }

    ctx.startThreads();
  }

  /**
   * Return the average update time per update for this trial
   *
   * @param uPerTrial the number of unique updates for this trial
   * @return the average update time per update for this trial
   */
  @Override
  public double doTrial(final int uPerTrial) {
    //reuse the same sketches
    sharedSketch.reset(); // reset shared sketch first
    ctx.reset();  // reset local sketches

    ctx.doTrial(uPerTrial);
    ctx.waitForAll();

    final long totalTimeNS = ctx.getTotalTimeNS();
    final long numWritesDone = ctx.getNumWriterThreads();
    return (double) totalTimeNS / numWritesDone;
  }

  //configures builder for both local and shared
  UpdateSketchBuilder configureBuilder() {
    final UpdateSketchBuilder bldr = new UpdateSketchBuilder();
    bldr.setNumPoolThreads(poolThreads);
    bldr.setLogNominalEntries(sharedLgK);
    bldr.setLocalLogNominalEntries(localLgK);
    bldr.setSeed(DEFAULT_UPDATE_SEED);
    bldr.setPropagateOrderedCompact(ordered);
    bldr.setMaxConcurrencyError(maxConcurrencyError);
    bldr.setMaxNumLocalThreads(numWriterThreads);
    return bldr;
  }

  @Override
  public void cleanup() {
    sharedSketch.reset();
    ctx.stopAllThreads();
  }

  protected boolean doSomethingWithEstimate(final double est) {
    return est < 1000;
  }

  private ReentrantReadWriteLock.WriteLock getWriteLock() {
    return lock.writeLock();
  }

  private ReentrantReadWriteLock.ReadLock getReadLock() {
    return  lock.readLock();
  }

  /**
   * Thread safe writer
   */
  protected class WriterThread extends ConcurrentTestThread {
    private UpdateSketch local;
    private final long start;
    private long i;
    private final int jump;

    //c-tor for thread-safe (concurrent) sketch
    public WriterThread(final UpdateSketchBuilder bldr, final long start) {
      this(start);
      local = bldr.buildLocal(sharedSketch);
    }

    //c-tor for lock-based sketch
    public WriterThread(final UpdateSketch sketch, final long start) {
      this(start);
      local = sketch;
    }

    private WriterThread(final long start) {
      super(ctx);
      this.start = start;
      i = start;
      jump = numWriterThreads;
    }

    @Override
    public void doWork() {
      local.update(i);
      i += jump;
    }

    @Override
    protected int getIndex() {
      return ConcurrentTestContext.WRITER_INDEX;
    }

    @Override
    public void reset() {
      local.reset();
      i = start;
      super.reset();
    }

    protected double getWrites() {
      return (double)i / jump;
    }
  }

  /**
   * Thread safe reader
   */
  protected class ReaderThread extends ConcurrentTestThread {

    public ReaderThread() {
      super(ctx);
    }

    @Override
    protected void doWork() {
      doSomethingWithEstimate(sharedSketch.getEstimate());
    }

    @Override
    protected int getIndex() {
      return ConcurrentTestContext.READER_INDEX;
    }

  }

  /**
   * Thread safe background reader
   */
  protected class BackgroundReaderThread extends ReaderThread {
    @Override
    protected void doWork() {
      try {
        Thread.sleep(1);
        super.doWork();
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * LockBased background reader
   */
  protected class LockBasedBackgroundReaderThread extends ReaderThread {
    @Override
    protected void doWork() {
      try {
        Thread.sleep(1);
        getReadLock().lock();
        super.doWork();
      } catch (final InterruptedException e) {
        e.printStackTrace();
      } finally {
        getReadLock().unlock();
      }
    }
  }

  /**
   *  Thread safe read-write thread
   */
  protected class ReaderWriterThread extends WriterThread {
    private final double myWritesRatio;
    private double reads;

    public ReaderWriterThread(final UpdateSketchBuilder bldr, final long start, final double writesRatio) {
      super(bldr, start);
      myWritesRatio = writesRatio;
      reads = 0;
    }

    @Override
    public void doWork() {
      final double writes = getWrites();
      final double reads = getReads();
      if ((writes / (writes + reads)) < myWritesRatio) {
        super.doWork();
      } else {
        doSomethingWithEstimate(sharedSketch.getEstimate());
        incReads();
      }
    }

    @Override
    public void reset() {
      reads = 0;
      super.reset();
    }

    protected double getReads() {
      return reads;
    }

    protected void incReads() {
      reads++;
    }

  }

  /**
   * Lock-based Writer thread
   */
  protected class LockBasedWriterThread extends WriterThread {

    public LockBasedWriterThread(final long start) {
      super(sharedSketch, start);
    }

    @Override
    public void doWork() {
      try {
        getWriteLock().lock();
        super.doWork();
      } finally {
        getWriteLock().unlock();
      }
    }
  }

  /**
   * Lock-based Reader-Writer thread
   */
  protected class LockBasedReaderWriterThread extends LockBasedWriterThread {
    private final double myWritesRatio;
    private double reads;

    public LockBasedReaderWriterThread(final long start, final double writesRatio) {
      super(start);
      myWritesRatio = writesRatio;
      reads = 0;
    }

    @Override
    public void doWork() {
      final double writes = getWrites();
      final double reads = getReads();
      if ((writes / (writes + reads)) < myWritesRatio) {
        super.doWork();
      } else {
        try {
          getReadLock().lock();
          doSomethingWithEstimate(sharedSketch.getEstimate());
          incReads();
        } finally {
          getReadLock().unlock();
        }
      }
    }

    @Override
    public void reset() {
      reads = 0;
      super.reset();
    }

    protected double getReads() {
      return reads;
    }

    protected void incReads() {
      reads++;
    }

  }

}
