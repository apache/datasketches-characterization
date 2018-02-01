package com.yahoo.sketches.characterization.quantiles;

import java.util.Arrays;
import java.util.Random;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.characterization.Properties;
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

  long buildTimeNs;
  long updateTimeNs;
  long updateGetQuantilesTimeNs;
  long updateGetCdfTimeNs;
  long updateGetRankTimeNs;
  long updateSerializeTimeNs;
  long updateDeserializeTimeNs;
  long updateSerializedSizeBytes;
  long compactTimeNs;
  long compactGetQuantilesTimeNs;
  long compactGetCdfTimeNs;
  long compactGetRankTimeNs;
  long compactSerializeTimeNs;
  long compactDeserializeTimeNs;
  long compactSerializedSizeBytes;

  @Override
  void configure(final int lgK, final int numQueryValues, final Properties properties) {
    this.lgK = lgK;
    this.numQueryValues = numQueryValues;
    useDirect = Boolean.parseBoolean(properties.mustGet("useDirect"));
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
      updateSketchMemory = WritableMemory
          .wrap(new byte[DoublesSketch.getUpdatableStorageBytes(1 << lgK, streamLength)]);
      compactSketchMemory = WritableMemory
          .wrap(new byte[DoublesSketch.getCompactStorageBytes(1 << lgK, streamLength)]);
    }
    resetStats();
  }

  @SuppressWarnings("unused")
  @Override
  void doTrial() {
    DoublesSketchAccuracyProfile.shuffle(inputValues);

    final long startBuild = System.nanoTime();
    final UpdateDoublesSketch updateSketch = useDirect
        ? builder.build(updateSketchMemory)
        : builder.build();
    final long stopBuild = System.nanoTime();
    buildTimeNs += stopBuild - startBuild;

    final long startUpdate = System.nanoTime();
    for (int i = 0; i < inputValues.length; i++) {
      updateSketch.update(inputValues[i]);
    }
    final long stopUpdate = System.nanoTime();
    updateTimeNs += stopUpdate - startUpdate;

    {
      final long startGetQuantiles = System.nanoTime();
      updateSketch.getQuantiles(numQueryValues);
      final long stopGetQuantiles = System.nanoTime();
      updateGetQuantilesTimeNs += stopGetQuantiles - startGetQuantiles;

      final long startGetCdf = System.nanoTime();
      updateSketch.getCDF(queryValues);
      final long stopGetCdf = System.nanoTime();
      updateGetCdfTimeNs += stopGetCdf - startGetCdf;

      final long startGetRank = System.nanoTime();
      for (final double value: queryValues) {
        //updateSketch.getRank(value); //TODO this was not released yet
        final double estRank = updateSketch.getCDF(new double[] {value})[0];
      }
      final long stopGetRank = System.nanoTime();
      updateGetRankTimeNs += stopGetRank - startGetRank;

      final long startSerialize = System.nanoTime();
      final byte[] bytes = updateSketch.toByteArray();
      final long stopSerialize = System.nanoTime();
      updateSerializeTimeNs += stopSerialize - startSerialize;

      final WritableMemory mem = WritableMemory.wrap(bytes);
      final long startDeserialize = System.nanoTime();
      if (useDirect) {
        UpdateDoublesSketch.wrap(mem);
      } else {
        UpdateDoublesSketch.heapify(mem);
      }
      final long stopDeserialize = System.nanoTime();
      updateDeserializeTimeNs += stopDeserialize - startDeserialize;

      // could record the last one since they must be the same
      // but let's average across all trials to see if there is an anomaly
      updateSerializedSizeBytes += bytes.length;
    }

    final long startCompact = System.nanoTime();
    final DoublesSketch compactSketch = useDirect
        ? updateSketch.compact(compactSketchMemory)
        : updateSketch.compact();
    final long stopCompact = System.nanoTime();
    compactTimeNs += stopCompact - startCompact;

    {
      final long startGetQuantiles = System.nanoTime();
      compactSketch.getQuantiles(numQueryValues);
      final long stopGetQuantiles = System.nanoTime();
      compactGetQuantilesTimeNs += stopGetQuantiles - startGetQuantiles;

      final long startGetCdf = System.nanoTime();
      compactSketch.getCDF(queryValues);
      final long stopGetCdf = System.nanoTime();
      compactGetCdfTimeNs += stopGetCdf - startGetCdf;

      final long startGetRank = System.nanoTime();
      for (final double value: queryValues) {
        //compactSketch.getRank(value); //TODO this was not released yet
        final double estRank = compactSketch.getCDF(new double[] {value})[0];
      }
      final long stopGetRank = System.nanoTime();
      compactGetRankTimeNs += stopGetRank - startGetRank;

      final long startSerialize = System.nanoTime();
      final byte[] bytes = compactSketch.toByteArray();
      final long stopSerialize = System.nanoTime();
      compactSerializeTimeNs += stopSerialize - startSerialize;

      final Memory mem = Memory.wrap(bytes);
      final long startDeserialize = System.nanoTime();
      if (useDirect) {
        DoublesSketch.wrap(mem);
      } else {
        DoublesSketch.heapify(mem);
      }
      final long stopDeserialize = System.nanoTime();
      compactDeserializeTimeNs += stopDeserialize - startDeserialize;

      // could record the last one since they must be the same
      // but let's average across all trials to see if there is an anomaly
      compactSerializedSizeBytes += bytes.length;
    }
  }

  @Override
  String getHeader() {
    return "Stream\tTrials\tBuild\tUpdate\tQuant\tCDF\tRank\tSer\tDeser\tSize"
        + "\tCompact\tQuant\tCDF\tRank\tSer\tDeser\tSize";
  }

  @Override
  String getStats(final int streamLength, final int numTrials, final int numQueryValues) {
    return(String.format("%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%d"
      + "\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%d",
      streamLength,
      numTrials,
      (double) buildTimeNs / numTrials,
      (double) updateTimeNs / numTrials / streamLength,
      (double) updateGetQuantilesTimeNs / numTrials / numQueryValues,
      (double) updateGetCdfTimeNs / numTrials / numQueryValues,
      (double) updateGetRankTimeNs / numTrials / numQueryValues,
      (double) updateSerializeTimeNs / numTrials,
      (double) updateDeserializeTimeNs / numTrials,
      updateSerializedSizeBytes / numTrials,
      (double) compactTimeNs / numTrials,
      (double) compactGetQuantilesTimeNs / numTrials / numQueryValues,
      (double) compactGetCdfTimeNs / numTrials / numQueryValues,
      (double) compactGetRankTimeNs / numTrials / numQueryValues,
      (double) compactSerializeTimeNs / numTrials,
      (double) compactDeserializeTimeNs / numTrials,
      compactSerializedSizeBytes / numTrials
    ));
  }

  private void resetStats() {
    buildTimeNs = 0;
    updateTimeNs = 0;
    updateGetQuantilesTimeNs = 0;
    updateGetCdfTimeNs = 0;
    updateGetRankTimeNs = 0;
    updateSerializeTimeNs = 0;
    updateDeserializeTimeNs = 0;
    updateSerializedSizeBytes = 0;
    compactTimeNs = 0;
    compactGetQuantilesTimeNs = 0;
    compactGetCdfTimeNs = 0;
    compactGetRankTimeNs = 0;
    compactSerializeTimeNs = 0;
    compactDeserializeTimeNs = 0;
    compactSerializedSizeBytes = 0;
  }

}
