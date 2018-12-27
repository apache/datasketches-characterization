/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

import static com.yahoo.sketches.Util.DEFAULT_UPDATE_SEED;


import com.yahoo.memory.WritableDirectHandle;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.characterization.concurrent.ConcurrentTestContext;
import com.yahoo.sketches.characterization.concurrent.ConcurrentTestThread;
import com.yahoo.sketches.theta.ConcurrentThetaBuilder;
import com.yahoo.sketches.theta.SharedThetaSketch;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;

/**
 * Test scalability of concurrent theta sketch - test with multiple writers/readers
 *
 * @author eshcar
 */
public class ConcurrentThetaMultithreadSpeedProfile extends BaseUpdateSpeedProfile {

  private static final int WRITER_INDEX = 0;
  private static final int READER_INDEX = 1;

  private SharedThetaSketch sharedSketch;
  private int sharedLgK;
  private int localLgK;
  private int cacheLimit;
  private boolean ordered;
  private boolean offHeap;
  private boolean sharedIsDirect;
  private WritableDirectHandle wdh;
  private WritableMemory wmem;

  private int numWriterThreads;
  private int numReaderThreads;
  private boolean isThreadSafe;
  private ConcurrentTestContext ctx;


  /**
   * Configure the sketch
   */
  @Override
  void configure() {
    //Configure Sketches
    sharedLgK = Integer.parseInt(prop.mustGet("LgK"));
    localLgK = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_localLgK"));
    cacheLimit = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_cacheLimit"));
    ordered = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_ordered"));
    offHeap = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_offHeap"));
    sharedIsDirect = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_sharedIsDirect"));
    numReaderThreads = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_numReaders"));
    numWriterThreads = Integer.parseInt(prop.mustGet("CONCURRENT_THETA_numWriters"));
    isThreadSafe = Boolean.parseBoolean(prop.mustGet("CONCURRENT_THETA_ThreadSafe"));

    final int maxSharedUpdateBytes = Sketch.getMaxUpdateSketchBytes(1 << sharedLgK);

    if (offHeap) {
      wdh = WritableMemory.allocateDirect(maxSharedUpdateBytes);
      wmem = wdh.get();
    } else {
      wmem = WritableMemory.allocate(maxSharedUpdateBytes);
    }
    final ConcurrentThetaBuilder bldr = configureBuilder();
    //must build shared first
    sharedSketch = bldr.build(wmem);

    ctx = new ConcurrentTestContext(2);
    for (int i = 0; i < numWriterThreads; i++) {
      WriterThread writer = new WriterThread(bldr, i);
      ctx.addThread(WRITER_INDEX, writer);
    }
    for (int i = 0; i < numReaderThreads; i++) {
      ReaderThread reader = new ReaderThread();
      ctx.addThread(READER_INDEX, reader);
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
  double doTrial(int uPerTrial) {
    //reuse the same sketches
    sharedSketch.resetShared(); // reset shared sketch first
    ctx.reset();  // reset local sketches

    ctx.doTrial(uPerTrial);
    ctx.waitForAll();

    final long totalTimeNS = ctx.getTotalTimeNS();
    final long numWritesDone = ctx.getThreadTypeCounter(WRITER_INDEX);
    final long numReadsDone = ctx.getThreadTypeCounter(READER_INDEX);
    return (double) totalTimeNS / numWritesDone + numReadsDone;
  }

  //configures builder for both local and shared
  ConcurrentThetaBuilder configureBuilder() {
    final ConcurrentThetaBuilder bldr = new ConcurrentThetaBuilder();
    bldr.setSharedLogNominalEntries(sharedLgK);
    bldr.setLocalLogNominalEntries(localLgK);
    bldr.setSeed(DEFAULT_UPDATE_SEED);
    bldr.setCacheLimit(cacheLimit);
    bldr.setPropagateOrderedCompact(ordered);
    bldr.setSharedIsDirect(sharedIsDirect);
    return bldr;
  }

  /**
   * Thread safe writer
   */
  public class WriterThread extends ConcurrentTestThread {
    private UpdateSketch local;
    private final long start;
    private long i;
    private final int jump;

    public WriterThread(ConcurrentThetaBuilder bldr, long start) {
      super(ctx);
      local = bldr.build();
      this.start = start;
      i = start;
      jump = numWriterThreads;
    }

    @Override
    public void doWork() {
      local.update(i);
      i+=jump;
    }

    @Override
    public int getIndex() {
      return WRITER_INDEX;
    }

    @Override
    public void reset() {
      super.reset();
      local.reset();
      i = start;
    }
  }

  /**
   * Thread safe reader
   */
  public class ReaderThread extends ConcurrentTestThread {


    public ReaderThread() {
      super(ctx);
    }

    @Override
    protected void doWork() {
      doSomethingWithEstimate(sharedSketch.getEstimationSnapshot());
    }

    @Override
    protected int getIndex() {
      return READER_INDEX;
    }

    protected boolean doSomethingWithEstimate(double est) {
      return est<1000;
    }
  }


}
