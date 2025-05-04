/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches;

/**
 * This is a special power law updating scheme that has the following properties:
 * <ul><li>The grand total weight of all updates is a power of 2.</li>
 * <li>The total weight of each row of updates at a specific weight is a power of 2.</li>
 * <li>Each specific weighted update has a weight that is a power of 2.</li>
 * <li>The number of update rows is a power of 2.</li>
 *</ul>
 *
 * <p>There are 5 schemes and you implement only one of these schemes:</p>
 * <table>
 * <caption><big><b>Scheme Details</b></big></caption>
 * <tbody>
 * <tr>
 * <td>&nbsp;<b>Scheme</b></td>
 * <td><b>WeightPerRow</b>&nbsp;</td>
 * <td><b>NumRows</b>&nbsp;</td>
 * <td><b>TotalSchemeWeight</b>&nbsp;</td>
 * </tr>
 * <tr>
 * <td>&nbsp;1</td>
 * <td>&nbsp;2</td>
 * <td>&nbsp;2</td>
 * <td>&nbsp;4</td>
 * </tr>
 * <tr>
 * <td>&nbsp;2</td>
 * <td>&nbsp;8</td>
 * <td>&nbsp;4</td>
 * <td>&nbsp;32</td>
 * </tr>
 * <tr>
 * <td>&nbsp;3</td>
 * <td>&nbsp;128</td>
 * <td>&nbsp;8</td>
 * <td>&nbsp;1,024</td>
 * </tr>
 * <tr>
 * <td>&nbsp;4</td>
 * <td>&nbsp;32,768</td>
 * <td>&nbsp;16</td>
 * <td>&nbsp;524,288</td>
 * </tr>
 * <tr>
 * <td>&nbsp;5</td>
 * <td>&nbsp;2,147,483,648</td>
 * <td>&nbsp;32</td>
 * <td>&nbsp;68,719,476,736</td>
 * </tr>
 * </tbody>
 * </table>
 *
 * @author Lee Rhodes
 */
public final class PowersOf2PowerLaw {
  //s or scheme is Scheme number
  //m is Log2(Row Weight) = 2^s -1
  //w is Row Weight = 2^m
  //r is Num Rows = 2^s
  //sw is Scheme Weight = 2^(m + s)

  /**
   * Gets the total weight of each row.
   * @param scheme the given scheme, 1 to 5.
   * @return the total weight of each row.
   */
  public static long getTotalRowWeight(final int scheme) {
    checkScheme(scheme);
    final long m = (1L << scheme) - 1L;
    return 1L << m;
  }

  /**
   * Gets the total Scheme Weight for all rows with their weights and repetitions.
   * @param scheme the given scheme, 1 to 5.
   * @return the total Scheme Weight for all rows with their weights and repetitions.
   */
  public static long getTotalSchemeWeight(final int scheme) {
    checkScheme(scheme);
    final long m = (1L << scheme) - 1L;
    return 1L << (m + scheme);
  }

  /**
   * Gets the number of rows for a given scheme.
   * @param scheme the given scheme, 1 to 5.
   * @return the number of rows for a given scheme.
   */
  public static int getNumRows(final int scheme) {
    checkScheme(scheme);
    return 1 << scheme;
  }

  /**
   * Gets the weight of each update for the given row.
   * @param row starting with zero, which has a weight of 1, row 1 has a weight of 2,
   * row n has a weight of 2^n.
   * @return the weight of updates for the given row.
   */
  public static long getRowWeight(final int row) {
    return 1L << row;
  }

  /**
   * Gets the number of repetitions for a given scheme and row.
   * @param scheme the given scheme, 1 to 5.
   * @param row starting with zero, which has a weight of 1, row 1 has a weight of 2,
   * row n has a weight of 2^n.
   * @return the number of repetitions for a given scheme and row.
   */
  public static long getRowReps(final int scheme, final int row) {
    checkScheme(scheme);
    final long rw = getTotalRowWeight(scheme);
    return rw >>> row;
  }

  private static void checkScheme(final int scheme) {
    if (scheme < 1 || scheme > 5) {
      throw new IllegalArgumentException("Scheme must be >= 1 and <= 5.");
    }
  }

}
