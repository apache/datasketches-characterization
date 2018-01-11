/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization;

import static com.yahoo.sketches.Util.pwr2LawNext;

/**
 * @author Lee Rhodes
 */
public class PerformanceUtil {
  //Quantile fractions computed from the standard normal cumulative distribution.
  public static final double M4SD = 0.0000316712418331; //minus 4 StdDev //0
  public static final double M3SD = 0.0013498980316301; //minus 3 StdDev //1
  public static final double M2SD = 0.0227501319481792; //minus 2 StdDev //2
  public static final double M1SD = 0.1586552539314570; //minus 1 StdDev //3
  public static final double MED  = 0.5; //median                        //4
  public static final double P1SD = 0.8413447460685430; //plus  1 StdDev //5
  public static final double P2SD = 0.9772498680518210; //plus  2 StdDev //6
  public static final double P3SD = 0.9986501019683700; //plus  3 StdDev //7
  public static final double P4SD = 0.9999683287581670; //plus  4 StdDev //8
  public static final double[] FRACTIONS =
    {0.0, M4SD, M3SD, M2SD, M1SD, MED, P1SD, P2SD, P3SD, P4SD, 1.0};
  public static final int FRACT_LEN = FRACTIONS.length;

  public static final int countPoints(final int lgStart, final int lgEnd, final int ppo) {
    int p = 1 << lgStart;
    final int end = 1 << lgEnd;
    int count = 0;
    while (p <= end) {
      p = pwr2LawNext(ppo, p);
      count++;
    }
    return count;
  }

}
