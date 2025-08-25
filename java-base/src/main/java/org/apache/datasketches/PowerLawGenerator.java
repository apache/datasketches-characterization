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

package org.apache.datasketches;

import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.lang.Math.exp;
import static java.lang.Math.floor;
import static java.lang.Math.log;
import static java.lang.Math.pow;

/**
 * A simple tool to generate (x,y) pairs of points that follow a power-law
 * relationship. This class provides both a convenient dynamic class with
 * simplified get methods or the equivalent static calls with full arguments.
 * The basic static methods can also be used to generate equally spaced points
 * for a single axis.
 *
 * <p>
 * The equal spacing is created using "generating indices" that are first
 * obtained from these methods and then used to generate the x-coordinates or
 * full (x,y) Pairs. Please refer to example code in the associated test class.
 * </p>
 *
 * @author Lee Rhodes
 */
public class PowerLawGenerator {

  // Inputs
  private int ppxb_; // points per Xbase
  private double xLogBase_;
  private DoublePair start_;
  private DoublePair end_;

  // Derived
  private int iStart_;
  private int iEnd_;
  private int numIdxs_;
  private double slope_;
  private int delta_;

  /**
   * Creates a convenient dynamic class used for generating (x,y) Pairs for a
   * power-law relation. This creates the intermediate mathematical values from
   * the given parameters will allow simplified gets with few or no parameters.
   *
   * @param xLogBase The logarithmic base for the x-coordinate
   * @param ptsPerXBase The desired resolution specified in number of equally spaced
   * points per power of the x logarithmic base. For example, if x is
   * log-base2 specifying 4 would result in 4 equally spaced points for
   * every power of 2 of x.
   * @param start the desired starting (x,y) Pair
   * @param end the desired ending (x,y) Pair
   */
  public PowerLawGenerator(final double xLogBase, final int ptsPerXBase, final DoublePair start,
      final DoublePair end) {
    xLogBase_ = xLogBase;
    ppxb_ = ptsPerXBase;
    start_ = start;
    end_ = end;

    slope_ = getSlope(start_, end_);
    iStart_ = getStartGenIndex(start_.x, end_.x, xLogBase_, ppxb_);
    iEnd_ = getEndGenIndex(start_.x, end_.x, xLogBase_, ppxb_);
    delta_ = getDelta(start_, end_);
    numIdxs_ = getNumGenIndices(start_, end_, xLogBase_, ppxb_);
  }

  /**
   * The total number of generating indices available given the start and end
   * Pairs.
   *
   * @return the number of generating indices.
   */
  public int getNumGenIndices() {
    return numIdxs_;
  }

  /**
   * Returns the generating index delta value. If start.x &lt; end.x then this
   * returns 1, else -1.
   *
   * @return the generating index delta value
   */
  public int getDelta() {
    return delta_;
  }

  /**
   * Returns the power-law slope derived from the start and end Pairs.
   *
   * @return the power-law slope derived from the start and end Pairs.
   */
  public double getSlope() {
    return slope_;
  }

  /**
   * Returns the starting generating index used to generate the start Point.
   *
   * @return the starting generating index used to obtain the start Point.
   */
  public int getStartGenIndex() {
    return iStart_;
  }

  /**
   * Returns the ending generating index used to generate the end Point.
   *
   * @return the ending generating index used to obtain the end Point.
   */
  public int getEndGenIndex() {
    return iEnd_;
  }

  /**
   * Returns a coordinate Pair based on the given generating index.
   *
   * @param genIndex the given generating index
   * @return a coordinate Pair based on the given generating index.
   */
  public DoublePair getPair(final int genIndex) {
    final double x = getX(genIndex, xLogBase_, ppxb_);
    final double y = getY(start_, slope_, x);
    return new DoublePair(x, y);
  }

  // Static method equivalents

  /**
   * The total number of generating indices available given the start and end
   * Pairs.
   *
   * @param start the desired starting (x,y) Pair
   * @param end the desired ending (x,y) Pair
   * @param xLogBase The logarithmic base for the x-coordinate
   * @param ptsPerXBase The desired resolution specified in number of equally spaced
   * points per power of the x logarithmic base. For example, if x is
   * log-base2 specifying 4 would result in 4 equally spaced points for
   * every power of 2 of x.
   * @return The total number of generating indices available given the start
   * and end Pairs.
   */
  public static int getNumGenIndices(final DoublePair start, final DoublePair end,
      final double xLogBase, final int ptsPerXBase) {
    final int iStrt = getStartGenIndex(start.x, end.x, xLogBase, ptsPerXBase);
    final int iEnd = getEndGenIndex(start.x, end.x, xLogBase, ptsPerXBase);
    return abs(iStrt - iEnd) + 1;
  }

  /**
   * Returns the generating index delta value. If start.x &lt; end.x then this
   * returns 1, else -1.
   *
   * @param start the desired starting (x,y) Pair
   * @param end the desired ending (x,y) Pair
   * @return the generating index delta value
   */
  public static int getDelta(final DoublePair start, final DoublePair end) {
    final boolean xIncreasing = end.x > start.x;
    return (xIncreasing) ? 1 : -1;
  }

  /**
   * Returns the power-law slope derived from the start and end Pairs.
   *
   * @param start the desired starting (x,y) Pair
   * @param end the desired ending (x,y) Pair
   * @return the power-law slope derived from the start and end Pairs
   */
  public static double getSlope(final DoublePair start, final DoublePair end) {
    return log(start.y / end.y) / log(start.x / end.x);
  }

  /**
   * Returns the starting generating index used to generate the starting value.
   *
   * @param start the desired starting value
   * @param end the desired ending value
   * @param logBase The logarithmic base for the x-coordinate
   * @param ptsPerLogBase The desired resolution specified in number of equally spaced
   * points per power of the x logarithmic base. For example, if x is
   * log-base2 specifying 4 would result in 4 equally spaced points for
   * every power of 2 of x.
   * @return the starting generating index used to generate the start value
   */
  public static int getStartGenIndex(final double start, final double end, final double logBase,
      final int ptsPerLogBase) {
    final boolean increasing = end > start;
    final double genIdx = getGenIndex(start, logBase, ptsPerLogBase);
    return (int) ((increasing) ? floor(genIdx) : ceil(genIdx));
  }

  /**
   * Returns the ending generating index used to generate the end value.
   *
   * @param start the desired starting value
   * @param end the desired ending value
   * @param logBase The logarithmic base for the x-coordinate
   * @param ptsPerLogBase The desired resolution specified in number of equally spaced
   * points per power of the x logarithmic base. For example, if x is
   * log-base2 specifying 4 would result in 4 equally spaced points for
   * every power of 2 of x.
   * @return the ending generating index used to generate the end value
   */
  public static int getEndGenIndex(final double start, final double end, final double logBase,
      final int ptsPerLogBase) {
    final boolean increasing = end > start;
    final double genIdx = getGenIndex(end, logBase, ptsPerLogBase);
    return (int) ((!increasing) ? floor(genIdx) : ceil(genIdx));
  }

  /**
   * Returns the generating index given x, the logBase and the number of points
   * per logBase.
   *
   * @param x the value at which the genIndex is to be computed.
   * @param logBase the logBase, either 10 or 2
   * @param ptsPerLogBase number of points per logBase interval (factor of 10 or 2)
   * @return the generating index as a double
   */
  public static double getGenIndex(final double x, final double logBase, final int ptsPerLogBase) {
    final double lnBase = log(logBase);
    return (ptsPerLogBase * log(x)) / lnBase;
  }

  /**
   * Returns the x-coordinate given the generating index.
   *
   * @param genIndex the given generating index
   * @param logBase The logarithmic base for the x-coordinate
   * @param ptsPerLogBase The desired resolution specified in number of equally spaced
   * points per power of the x logarithmic base. For example, if x is
   * log-base2 specifying 4 would result in 4 equally spaced points for
   * every power of 2 of x.
   * @return the x-coordinate based on the generating index
   */
  public static double getX(final int genIndex, final double logBase, final int ptsPerLogBase) {
    return pow(logBase, ((double) genIndex) / ptsPerLogBase);
  }

  /**
   * Returns the y-coordinate given the computed x-coordinate and slope.
   *
   * @param start the desired starting (x,y) Pair
   * @param slope the given slope
   * @param x the computed x-coordinate
   * @return the y-coordinate
   */
  public static double getY(final DoublePair start, final double slope, final double x) {
    return start.y * exp(slope * log(x / start.x));
  }

}
