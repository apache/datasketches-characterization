package org.apache.datasketches.characterization.quantiles.tdigest;

public class DoubleRankCalculator {

  public enum Mode { Min, Mid, Max }

  private final double[] values;
  private final Mode mode;
  private int nLess;
  private int nLessOrEq;

  // assumes that values are sorted
  public DoubleRankCalculator(final double[] values, final Mode mode) {
    this.values = values;
    this.mode = mode;
  }

  public double getRank(final double value) {
    if (Mode.Min.equals(mode) || Mode.Mid.equals(mode)) {
      while ((nLess < values.length) && (values[nLess] < value)) {
        nLess++;
      }
    }
    if (Mode.Max.equals(mode) || Mode.Mid.equals(mode)) {
      while ((nLessOrEq < values.length) && (values[nLessOrEq] <= value)) {
        nLessOrEq++;
      }
    }
    if (Mode.Min.equals(mode)) { return (double) nLess / values.length; }
    if (Mode.Max.equals(mode)) { return (double) nLessOrEq / values.length; }
    return (nLess + nLessOrEq) / 2.0 / values.length;
  }

}
