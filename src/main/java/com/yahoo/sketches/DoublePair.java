/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import java.util.Comparator;

public class DoublePair implements Comparator<DoublePair> {
  public double x;
  public double y;

  public DoublePair(final double x, final double y) {
    this.x = x;
    this.y = y;
  }

  @Override
  public int compare(final DoublePair p1, final DoublePair p2) {
    return Double.compare(p1.x, p2.x); // handles NaN, +/- 0, etc.
  }
}
