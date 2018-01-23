package com.yahoo.sketches.characterization.quantiles;

import java.util.Arrays;
import java.util.Random;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

public class DoublesSketchSpeedProfile extends QuantilesSpeedProfile {

  private static final Random rnd = new Random();
  private int lgK;
  private DoublesSketchBuilder builder;
  private double[] inputValues;
  private int numQueryValues;
  private double[] queryValues;
  private boolean useDirect;
  private WritableMemory updateSketchMemory;
  private WritableMemory compactSketchMemory;

  @Override
  void configure(final int lgK, final int numQueryValues, final boolean useDirect) {
    this.lgK = lgK;
    this.numQueryValues = numQueryValues;
    this.useDirect = useDirect;
    builder = DoublesSketch.builder().setK(1 << lgK);
  }

  @Override
  void prepareTrial(final int streamLength) {
    // prepare input data
    inputValues = new double[streamLength];
    for (int i = 0; i < streamLength; i++) {
      inputValues[i] = rnd.nextDouble();
    }
    // prepare query data that must be ordered
    queryValues = new double[numQueryValues];
    for (int i = 0; i < numQueryValues; i++) {
      queryValues[i] = rnd.nextDouble();
    }
    Arrays.sort(queryValues);
    if (useDirect) {
      updateSketchMemory = WritableMemory.wrap(new byte[DoublesSketch.getUpdatableStorageBytes(1 << lgK, streamLength)]);
      compactSketchMemory = WritableMemory.wrap(new byte[DoublesSketch.getCompactStorageBytes(1 << lgK, streamLength)]);
    }
  }

  @Override
  void doTrial(final SpeedStats stats) {
    DoublesSketchAccuracyProfile.shuffle(inputValues);

    final long startBuild = System.nanoTime();
    final UpdateDoublesSketch updateSketch = useDirect ? builder.build(updateSketchMemory) : builder.build();
    final long stopBuild = System.nanoTime();
    stats.buildTimeNs += stopBuild - startBuild;

    final long startUpdate = System.nanoTime();
    for (int i = 0; i < inputValues.length; i++) {
      updateSketch.update(inputValues[i]);
    }
    final long stopUpdate = System.nanoTime();
    stats.updateTimeNs += stopUpdate - startUpdate;

    {
      final long startGetQuantiles = System.nanoTime();
      updateSketch.getQuantiles(numQueryValues);
      final long stopGetQuantiles = System.nanoTime();
      stats.updateGetQuantilesTimeNs += stopGetQuantiles - startGetQuantiles;
  
      final long startGetCdf = System.nanoTime();
      updateSketch.getCDF(queryValues);
      final long stopGetCdf = System.nanoTime();
      stats.updateGetCdfTimeNs += stopGetCdf - startGetCdf;
  
      final long startGetRank = System.nanoTime();
      for (final double value: queryValues) {
        updateSketch.getRank(value); // this was not released yet
        //final double estRank = updateSketch.getCDF(new double[] {value})[0];
      }
      final long stopGetRank = System.nanoTime();
      stats.updateGetRankTimeNs += stopGetRank - startGetRank;
  
      final long startSerialize = System.nanoTime();
      final byte[] bytes = updateSketch.toByteArray();
      final long stopSerialize = System.nanoTime();
      stats.updateSerializeTimeNs += stopSerialize - startSerialize;
  
      final WritableMemory mem = WritableMemory.wrap(bytes);
      final long startDeserialize = System.nanoTime();
      if (useDirect) {
        UpdateDoublesSketch.wrap(mem);
      } else {
        UpdateDoublesSketch.heapify(mem);
      }
      final long stopDeserialize = System.nanoTime();
      stats.updateDeserializeTimeNs += stopDeserialize - startDeserialize;
    }

    final long startCompact = System.nanoTime();
    final DoublesSketch compactSketch = useDirect ? updateSketch.compact(compactSketchMemory) : updateSketch.compact();
    final long stopCompact = System.nanoTime();
    stats.compactTimeNs += stopCompact - startCompact;

    {
      final long startGetQuantiles = System.nanoTime();
      compactSketch.getQuantiles(numQueryValues);
      final long stopGetQuantiles = System.nanoTime();
      stats.compactGetQuantilesTimeNs += stopGetQuantiles - startGetQuantiles;
  
      final long startGetCdf = System.nanoTime();
      compactSketch.getCDF(queryValues);
      final long stopGetCdf = System.nanoTime();
      stats.compactGetCdfTimeNs += stopGetCdf - startGetCdf;
  
      final long startGetRank = System.nanoTime();
      for (final double value: queryValues) {
        compactSketch.getRank(value); // this was not released yet
        //final double estRank = compactSketch.getCDF(new double[] {value})[0];
      }
      final long stopGetRank = System.nanoTime();
      stats.compactGetRankTimeNs += stopGetRank - startGetRank;
  
      final long startSerialize = System.nanoTime();
      final byte[] bytes = compactSketch.toByteArray();
      final long stopSerialize = System.nanoTime();
      stats.compactSerializeTimeNs += stopSerialize - startSerialize;
  
      final Memory mem = Memory.wrap(bytes);
      final long startDeserialize = System.nanoTime();
      if (useDirect) {
        DoublesSketch.wrap(mem);
      } else {
        DoublesSketch.heapify(mem);
      }
      final long stopDeserialize = System.nanoTime();
      stats.compactDeserializeTimeNs += stopDeserialize - startDeserialize;
    }
  }

}
