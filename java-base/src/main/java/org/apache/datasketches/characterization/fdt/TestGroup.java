/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.characterization.fdt;

import java.util.regex.Pattern;

import org.apache.datasketches.fdt.Group;

/**
 * Note: this class has a natural ordering that is inconsistent with equals.
 * @author Lee Rhodes
 */
public class TestGroup extends Group {
  private int xG;
  private int yU;
  private double err;
  private double ubErr;
  private double lbErr;
  private char sep = '|'; //This is a hack
  private String sepr = Pattern.quote(Character.toString(sep));

  private final static String fmt2 =
      "%12d" + "%12.2f" + "%12.2f" + "%12.2f" + "%12.6f" + "%12.6f" + " %15s"
    + "%12d" + "%12d"   + "%12.6f" + "%12.6f" + "%12.6f";
  private final static String hfmt2 =
      "%12s" + "%12s"   + "%12s"   + "%12s"   + "%12s"   + "%12s"   + " %15s"
    + "%12s" + "%12s"   + "%12s"   + "%12s"   + "%12s";

  public TestGroup() { }

  @Override
  public TestGroup init(final String priKey, final int count, final double estimate, final double ub,
      final double lb, final double fraction, final double rse) {
    super.init(priKey, count, estimate, ub, lb, fraction, rse);
    final String[] priKeyArr = priKey.split(sepr);
    xG = Integer.parseInt(priKeyArr[0]);
    yU = Integer.parseInt(priKeyArr[2]);
    err =  (estimate / yU) - 1.0;
    ubErr = (ub / estimate) - 1.0;
    lbErr = (lb / estimate) - 1.0;
    return this;
  }

  public int getYU() {
    return yU;
  }

  public double getErr() {
    return err;
  }

  public double getUbErr() {
    return ubErr;
  }

  public double getLbErr() {
    return lbErr;
  }

  @Override
  public String getHeader() {
    return String.format(hfmt2,"Count", "Est", "UB", "LB", "Thresh", "RSE", "PriKey",
        "xG", "yU", "Err", "UBErr", "LBErr");
  }

  @Override
  public String toString() {
    return String.format(fmt2, super.getCount(), getEstimate(), super.getUpperBound(),
        super.getLowerBound(), super.getFraction(), super.getRse(), getPrimaryKey(),
        xG, yU, err, ubErr, lbErr);
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
    final String[] s1 = priKey.toString().split(sepr);
    final int[] thisPK = { Integer.parseInt(s1[0]), Integer.parseInt(s1[1]), Integer.parseInt(s1[2]) };
    final String[] s2 = thatPriKey.toString().split(sepr);
    final int[] thatPK = { Integer.parseInt(s2[0]), Integer.parseInt(s2[1]), Integer.parseInt(s2[2]) };
    if (thatCount != count) { return thatCount - count; } //decreasing
    if (thisPK[2] != thatPK[2]) { return thatPK[2] - thisPK[2]; } //decreasing
    if (thisPK[0] != thatPK[0]) { return thisPK[0] - thatPK[0]; } //increasing
    if (thisPK[1] != thatPK[1]) { return thisPK[1] - thatPK[1]; } //increasing
    return 0;
  }
}
