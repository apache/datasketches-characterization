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

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings({"static-access", "unused"})
public class PowersOf2PowerLawTest {
  private static final  PowersOf2PowerLaw PL = new PowersOf2PowerLaw();

  @Test
  public void checkTotalRowWeight() {
    for (int i = 1; i <= 5; i++) {
      long m = (1L << i) - 1L;
      long trw = 1L << m;
      println(i + ", " + trw);
      assertEquals(PL.getTotalRowWeight(i), trw);
    }
  }

  @Test
  public void checkTotalSchemeWeight() {
    for (int i = 1; i <= 5; i++) {
      long m = (1L << i) - 1L;
      long tsw = 1L << (m + i);
      println(i + ", " + tsw);
      assertEquals(PL.getTotalSchemeWeight(i), tsw);
    }
  }

  @Test
  public void checkNumRows() {
    for (int i = 1; i <= 5; i++) {
      int rows = 1 << i;
      println(i + ", " + rows);
      assertEquals(PL.getNumRows(i), rows);
    }
  }

  @Test
  public void checkRowWeight() {
    for (int i = 0; i <= 31; i++) {
      long rw = 1L << i;
      println(i + ", " + rw);
      assertEquals(PL.getRowWeight(i), rw);
    }
  }

  @Test
  public void checkRowReps() {
    for (int s = 1; s <= 5; s++) {
      println("scheme = " + s);
      int numRows = 1 << s;
      for (int row = 0; row < numRows; row++) {
        long m = (1L << s) - 1L;
        long trw = 1L << m;
        long reps = trw >>> row;
        println(row + ", " + reps);
        assertEquals(PL.getRowReps(s, row), reps);
      }
      println("");
    }
  }

  private final static boolean enablePrinting = false;

  /**
   * @param format the format
   * @param args the args
   */
  private static final void printf(final String format, final Object ... args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { System.out.println(o.toString()); }
  }

}
