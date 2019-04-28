package com.yahoo.sketches.characterization.quantiles;

import java.util.Random;

import com.yahoo.sketches.Properties;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

public class DoublesSketchAccuracyProfile extends BaseQuantilesAccuracyProfile {

  private DoublesSketchBuilder builder;
  private double[] inputValues;
  private double[] queryValues;
  private boolean useCompact;
  private boolean useBulk;

  @Override
  void configure(final Properties props) {
    final int lgK = Integer.parseInt(props.mustGet("lgK"));
    builder = DoublesSketch.builder().setK(1 << lgK);
    useCompact = Boolean.parseBoolean(props.mustGet("useCompact"));
    useBulk = Boolean.parseBoolean(props.mustGet("useBulk"));
  }

  @Override
  void prepareTrial(final int streamLength) {
    // prepare input data that will be permuted
    inputValues = new double[streamLength];
    for (int i = 0; i < streamLength; i++) {
      inputValues[i] = i;
    }
    if (useBulk) {
      // prepare query data that must remain ordered
      queryValues = new double[streamLength];
      for (int i = 0; i < streamLength; i++) {
        queryValues[i] = i;
      }
    }
  }

  @Override
  double doTrial() {
    shuffle(inputValues);

    // build sketch
    final UpdateDoublesSketch updateSketch = builder.build();
    for (int i = 0; i < inputValues.length; i++) {
      updateSketch.update(inputValues[i]);
    }

    final DoublesSketch sketch = useCompact ? updateSketch.compact() : updateSketch;

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
        final double estRank = sketch.getCDF(new double[] {i})[0];
        maxRankError = Math.max(maxRankError, Math.abs(trueRank - estRank));
      }
    }
    return maxRankError;
  }

  static final Random rnd = new Random();

  static void shuffle(final double[] array) {
    for (int i = 0; i < array.length; i++) {
      final int r = rnd.nextInt(i + 1);
      swap(array, i, r);
    }
  }

  private static void swap(final double[] array, final int i1, final int i2) {
    final double value = array[i1];
    array[i1] = array[i2];
    array[i2] = value;
  }

}
