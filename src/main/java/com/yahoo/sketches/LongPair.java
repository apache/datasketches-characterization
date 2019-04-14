/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import java.util.Comparator;

public class LongPair implements Comparator<LongPair> {
  public long x;
  public long y;

  public LongPair(final long x, final long y) {
    this.x = x;
    this.y = y;
  }

  @Override
  public int compare(final LongPair p1, final LongPair p2) {
    return (p1.x < p2.x) ? -1 : (p1.x > p2.x) ? 1 : 0;
  }
}
