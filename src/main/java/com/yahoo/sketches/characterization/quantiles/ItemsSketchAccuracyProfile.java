package com.yahoo.sketches.characterization.quantiles;

import java.util.Comparator;
import java.util.Random;

import com.yahoo.sketches.characterization.Properties;
import com.yahoo.sketches.quantiles.ItemsSketch;

public class ItemsSketchAccuracyProfile extends BaseQuantilesAccuracyProfile {

  private int lgK;
  private int[] inputValues;
  private Integer[] queryValues;
  private boolean useBulk;

  @Override
  void configure(final Properties props) {
    lgK = Integer.parseInt(props.mustGet("lgK"));
  }

  @Override
  void prepareTrial(final int streamLength) {
    // prepare input data that will be permuted
    inputValues = new int[streamLength];
    for (int i = 0; i < streamLength; i++) {
      inputValues[i] = i;
    }
    if (useBulk) {
      // prepare query data that must remain ordered
      queryValues = new Integer[streamLength];
      for (int i = 0; i < streamLength; i++) {
        queryValues[i] = i;
      }
    }
  }

  @Override
  double doTrial() {
    shuffle(inputValues);

    // build sketch
    final ItemsSketch<Integer> sketch = ItemsSketch.getInstance(1 << lgK, Comparator.naturalOrder());
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
        //final double estRank = sketch.getRank(i); // this was not released yet
        final double estRank = sketch.getCDF(new Integer[] {i})[0];
        maxRankError = Math.max(maxRankError, Math.abs(trueRank - estRank));
      }
    }
    return maxRankError;
  }

  static final Random rnd = new Random();

  private static void shuffle(final int[] array) {
    for (int i = 0; i < array.length; i++) {
      final int r = rnd.nextInt(i + 1);
      swap(array, i, r);
    }
  }

  private static void swap(final int[] array, final int i1, final int i2) {
    final int value = array[i1];
    array[i1] = array[i2];
    array[i2] = value;
  }

}
