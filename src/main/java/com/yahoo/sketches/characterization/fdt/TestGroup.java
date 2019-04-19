/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.fdt;

import com.yahoo.sketches.fdt.Group;

/**
 * Note: this class has a natural ordering that is inconsistent with equals.
 * @author Lee Rhodes
 */
public class TestGroup extends Group {

  private final static String fmt2 =
      "%,12d" + "%,15.2f" + "%,15.2f" + "%,15.2f" + "%12.6f" + "%12.6f" + " %15s" + "%12.6f";
  private final static String hfmt2 =
      "%12s"  + "%15s"    + "%15s"    + "%15s"    + "%12s"   + "%12s"   + " %15s" + "%12s";

  public TestGroup() { }

  @Override
  public TestGroup copy() {
    return new TestGroup();
  }

  @Override
  public TestGroup init(final String priKey, final int count, final double estimate, final double ub,
      final double lb, final double thresh, final double rse) {
    super.init(priKey, count, estimate, ub, lb, thresh, rse);
    return this;
  }


  @Override
  public String getRowHeader() {
    return String.format(hfmt2,"Count", "Est", "UB", "LB", "Thresh", "RSE", "PriKey", "Err");
  }

  @Override
  public String toString() {
    final String priKeyStr = super.getPrimaryKey().toString();
    final int yU = Integer.parseInt(priKeyStr.split(",")[2]);
    final double est = super.getEstimate();
    final double err = (est / yU) - 1.0;
    return String.format(fmt2, super.getCount(), est, super.getUpperBound(), super.getLowerBound(),
        super.getThreshold(), super.getRse(), priKeyStr, err);
  }

  /**
   * Note: this class has a natural ordering that is inconsistent with equals.
   * Ignore FindBugs warning on this issue.
   * @param that the Group to compare to
   */
  @Override
  public int compareTo(final Group that) {
    final String priKey = super.getPrimaryKey();
    final String thatPriKey = that.getPrimaryKey();
    final int count = super.getCount();
    final int thatCount = that.getCount();
    final String[] s1 = priKey.toString().split(",");
    final int[] thisPK = { Integer.parseInt(s1[0]), Integer.parseInt(s1[1]), Integer.parseInt(s1[2]) };
    final String[] s2 = thatPriKey.toString().split(",");
    final int[] thatPK = { Integer.parseInt(s2[0]), Integer.parseInt(s2[1]), Integer.parseInt(s2[2]) };
    if (thatCount != count) { return thatCount - count; } //decreasing
    if (thisPK[0] != thatPK[0]) { return thisPK[0] - thatPK[0]; } //increasing
    if (thisPK[1] != thatPK[1]) { return thisPK[1] - thatPK[1]; } //increasing
    if (thisPK[2] != thatPK[2]) { return thisPK[2] - thatPK[2]; } //increasing
    return 0;
  }
}