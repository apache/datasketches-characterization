package org.apache.datasketches.characterization.quantiles;

import java.nio.ByteBuffer;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.datasketches.characterization.Properties;

public class TDigestSpeedProfile extends QuantilesSpeedProfile {

  private static final ByteBuffer buf = ByteBuffer.allocate(10000); // for serialization, more capacity than necessary
  private int k;
  private double[] inputValues;
  private int numQueryValues;
  private double[] queryValues;
  private DataGenerator gen;

  long buildTimeNs;
  long updateTimeNs;
  long getQuantileTimeNs;
  long getRankTimeNs;
  long serializeTimeNs;
  long deserializeTimeNs;
  long smallSerializeTimeNs;
  long smallDeserializeTimeNs;
  long numRetainedItems;
  long serializedSizeBytes;
  long smallSerializedSizeBytes;

  @Override
  void configure(final int k, final int numQueryValues, final Properties properties) {
    this.k = k;
    this.numQueryValues = numQueryValues;
    gen = new DataGenerator(DataGenerator.Mode.Uniform);
  }

  @Override
  void prepareTrial(final int streamLength) {
    inputValues = new double[streamLength];
    queryValues = new double[numQueryValues];
    resetStats();
  }

  @SuppressWarnings("unused")
  @Override
  void doTrial() {
    gen.fillArray(inputValues);
    gen.fillArray(queryValues);

    final long startBuild = System.nanoTime();
    final TDigest sketch = TDigest.createDigest(k);
    final long stopBuild = System.nanoTime();
    buildTimeNs += stopBuild - startBuild;

    final long startUpdate = System.nanoTime();
    for (int i = 0; i < inputValues.length; i++) {
      sketch.add(inputValues[i]);
    }
    final long stopUpdate = System.nanoTime();
    updateTimeNs += stopUpdate - startUpdate;

    final long startGetQuantile = System.nanoTime();
    for (final double value: queryValues) {
      sketch.quantile(value);
    }
    final long stopGetQuantile = System.nanoTime();
    getQuantileTimeNs += stopGetQuantile - startGetQuantile;

    final long startGetRank = System.nanoTime();
    for (final double value: queryValues) {
      sketch.cdf(value);
    }
    final long stopGetRank = System.nanoTime();
    getRankTimeNs += stopGetRank - startGetRank;

    buf.rewind();
    final long startSerialize = System.nanoTime();
    sketch.asBytes(buf);
    final long stopSerialize = System.nanoTime();
    serializeTimeNs += stopSerialize - startSerialize;
    buf.rewind();
    final long startDeserialize = System.nanoTime();
    MergingDigest.fromBytes(buf);
    final long stopDeserialize = System.nanoTime();
    deserializeTimeNs += stopDeserialize - startDeserialize;
    buf.rewind();
    final long startSmallSerialize = System.nanoTime();
    sketch.asSmallBytes(buf);
    final long stopSmallSerialize = System.nanoTime();
    smallSerializeTimeNs += stopSmallSerialize - startSmallSerialize;
    buf.rewind();
    final long startSmallDeserialize = System.nanoTime();
    MergingDigest.fromBytes(buf);
    final long stopSmallDeserialize = System.nanoTime();
    smallDeserializeTimeNs += stopSmallDeserialize - startSmallDeserialize;

    // could record the last one since they must be the same
    // but let's average across all trials to see if there is an anomaly
    numRetainedItems += sketch.centroidCount();
    serializedSizeBytes += sketch.byteSize();
    smallSerializedSizeBytes += sketch.smallByteSize();
  }

  @Override
  String getHeader() {
    return "Stream\tTrials\tBuild\tUpdate\tQuant\tCDF\tSer\tDe\tSer\tDe\tItems\tSize\tSmall";
  }

  @Override
  String getStats(final int streamLength, final int numTrials, final int numQueryValues) {
    return(String.format("%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%d\t%d\t%d",
      streamLength,
      numTrials,
      (double) buildTimeNs / numTrials,
      (double) updateTimeNs / numTrials / streamLength,
      (double) getQuantileTimeNs / numTrials / numQueryValues,
      (double) getRankTimeNs / numTrials / numQueryValues,
      (double) serializeTimeNs / numTrials,
      (double) deserializeTimeNs / numTrials,
      (double) smallSerializeTimeNs / numTrials,
      (double) smallDeserializeTimeNs / numTrials,
      numRetainedItems / numTrials,
      serializedSizeBytes / numTrials,
      smallSerializedSizeBytes / numTrials
    ));
  }

  private void resetStats() {
    buildTimeNs = 0;
    updateTimeNs = 0;
    getQuantileTimeNs = 0;
    getRankTimeNs = 0;
    numRetainedItems = 0;
    serializeTimeNs = 0;
    deserializeTimeNs = 0;
    smallSerializeTimeNs = 0;
    smallDeserializeTimeNs = 0;
    serializedSizeBytes = 0;
    smallSerializedSizeBytes = 0;
  }

}
