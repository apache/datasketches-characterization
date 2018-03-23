package com.yahoo.sketches.characterization.quantiles;

import java.util.Random;

import com.yahoo.sketches.characterization.Properties;
import com.yahoo.sketches.kll.KllFloatsSketch;

public class KllFloatsSketchAccuracyProfile extends QuantilesAccuracyProfile {

  private int k;
  private float[] inputValues;
  private float[] queryValues;
  private boolean useBulk;
  private boolean measureRankError;

  @Override
  void configure(final Properties props) {
    k = Integer.parseInt(props.mustGet("K"));
    useBulk = Boolean.parseBoolean(props.mustGet("useBulk"));
    measureRankError = Boolean.parseBoolean(props.mustGet("rank"));
  }

  @Override
  void prepareTrial(final int streamLength) {
    // prepare input data that will be permuted
    inputValues = new float[streamLength];
    for (int i = 0; i < streamLength; i++) {
      inputValues[i] = i;
    }
    if (useBulk) {
      // prepare query data that must remain ordered
      queryValues = new float[streamLength];
      for (int i = 0; i < streamLength; i++) {
        queryValues[i] = i;
      }
    }
  }

  @Override
  double doTrial() {
    shuffle(inputValues);

    // build sketch
    final KllFloatsSketch sketch = new KllFloatsSketch(k);
    for (int i = 0; i < inputValues.length; i++) {
      sketch.update(inputValues[i]);
    }

    // query sketch and gather results
    double maxRankError = 0;
    if (useBulk) {
      final double[] estRanks = sketch.getCDF(queryValues);
      for (int i = 0; i < inputValues.length; i++) {
        final double trueRank = (double) i / inputValues.length;
        maxRankError = Math.max(maxRankError, Math.abs(trueRank - estRanks[i]));
      }
    } else {
      for (int i = 0; i < inputValues.length; i++) {
        final double trueRank = (double) i / inputValues.length;
        final double estRank = sketch.getRank(i);
        maxRankError = Math.max(maxRankError, Math.abs(trueRank - estRank));
      }
    }
    return maxRankError;
  }

  static final Random rnd = new Random();

  static void shuffle(final float[] array) {
    for (int i = 0; i < array.length; i++) {
      final int r = rnd.nextInt(i + 1);
      swap(array, i, r);
    }
  }

  private static void swap(final float[] array, final int i1, final int i2) {
    final float value = array[i1];
    array[i1] = array[i2];
    array[i2] = value;
  }

}
