package org.apache.datasketches.characterization.quantiles.tdigest;

import java.util.Random;

public class DataGenerator {

  public static enum Mode { Same, Blocky, Uniform, Gaussian }

  // starting probability of incrementing a value for Blocky mode
  private static final double PROBABILITY_OF_INCREMENT = 0.001;
  // factor of decrising the probability of incrementing a value for Blocky mode
  private static final double DECREASE_FACTOR = 0.98;

  private static final Random rnd = new Random();

  private final Mode mode;

  public DataGenerator(final Mode mode) {
    this.mode = mode;
  }

  public void fillArray(final float[] array) {
    int i = 0;
    int value = 1;
    double p = PROBABILITY_OF_INCREMENT;
    while (i < array.length) {
      if (Mode.Gaussian.equals(mode)) {
        array[i++] = (float) rnd.nextGaussian();
      } else if (Mode.Uniform.equals(mode)) {
        array[i++] = rnd.nextFloat();
      } else {
        array[i++] = value;
        // growing blocks of repeated values
        if (Mode.Blocky.equals(mode) && (rnd.nextDouble() < p)) {
          value++;
          p *= DECREASE_FACTOR; // decrease the probability slightly so that blocks get longer
        }
      }
    }
  }

  public void fillArray(final double[] array) {
    int i = 0;
    int value = 1;
    double p = 0.001; // starting probability of incrementing a value for Blocky mode
    while (i < array.length) {
      if (Mode.Gaussian.equals(mode)) {
        array[i++] = rnd.nextGaussian();
      } else if (Mode.Uniform.equals(mode)) {
        array[i++] = rnd.nextDouble();
      } else {
        array[i++] = value;
        // growing blocks of repeated values
        if (Mode.Blocky.equals(mode) && (rnd.nextDouble() < p)) {
          value++;
          p *= .98; // decrease the probability slightly so that blocks get longer
        }
      }
    }
  }

}
