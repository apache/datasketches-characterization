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

/**
 * Provides simple alignment and fill functions for strings, boolean,integer
 * primitives and double in fixed-width character fields.
 * @author Lee Rhodes
 */
public class Align {
  private static final String NULL = "null";

  //LEFT

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with space characters, if needed. If arg (as a String) is longer than the
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @return left justified arg
   */
  public static String left(final String arg, final int colwidth) {
    return left(arg, colwidth, ' ');
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @param pad The pad character to fill any positions to the right of the
   * given argument, if needed.
   * @return left justified arg
   */
  public static String left(final String arg, final int colwidth, final char pad) {
    if (colwidth < 1) { return ""; }
    final String s = (arg == null) ? NULL : arg;
    final int slen = s.length();
    final char[] buf = new char[colwidth];
    int pads = colwidth - slen;
    if (pads > 0) {
      s.getChars(0, slen, buf, 0);
      for (int i = colwidth; i-- > slen;) {
        buf[i] = pad;
      }
    }
    else {
      pads = -pads;
      s.getChars(0, colwidth, buf, 0);
    }
    return new String(buf);
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with space characters, if needed. If arg (as a String) is longer than the
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @return left justified arg
   */
  public static String left(final long arg, final int colwidth) {
    return left(Long.toString(arg), colwidth, ' ');
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @param pad The pad character to fill any positions to the right of the
   * given argument, if needed.
   * @return left justified arg
   */
  public static String left(final long arg, final int colwidth, final char pad) {
    return left(Long.toString(arg), colwidth, pad);
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with space characters, if needed. If arg (as a String) is longer than the
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @return left justified arg
   */
  public static String left(final int arg, final int colwidth) {
    return left(Integer.toString(arg), colwidth, ' ');
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @param pad The pad character to fill any positions to the right of the
   * given argument, if needed.
   * @return left justified arg
   */
  public static String left(final int arg, final int colwidth, final char pad) {
    return left(Integer.toString(arg), colwidth, pad);
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with space characters, if needed. If arg (as a String) is longer than the
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @return left justified arg
   */
  public static String left(final short arg, final int colwidth) {
    return left(Short.toString(arg), colwidth, ' ');
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @param pad The pad character to fill any positions to the right of the
   * given argument, if needed.
   * @return left justified arg
   */
  public static String left(final short arg, final int colwidth, final char pad) {
    return left(Short.toString(arg), colwidth, pad);
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with space characters, if needed. If arg (as a String) is longer than the
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @return left justified arg
   */
  public static String left(final byte arg, final int colwidth) {
    return left(Byte.toString(arg), colwidth, ' ');
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @param pad The pad character to fill any positions to the right of the
   * given argument, if needed.
   * @return left justified arg
   */
  public static String left(final byte arg, final int colwidth, final char pad) {
    return left(Byte.toString(arg), colwidth, pad);
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with space characters, if needed. If arg (as a String) is longer than the
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @return left justified arg
   */
  public static String left(final boolean arg, final int colwidth) {
    return left(Boolean.toString(arg), colwidth, ' ');
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @param pad The pad character to fill any positions to the right of the
   * given argument, if needed.
   * @return left justified arg
   */
  public static String left(final boolean arg, final int colwidth, final char pad) {
    return left(Boolean.toString(arg), colwidth, pad);
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with space characters, if needed. If arg (as a String) is longer than the
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @return left justified arg
   */
  public static String left(final double arg, final int colwidth) {
    return left(Double.toString(arg), colwidth, ' ');
  }

  /**
   * Left justifies arg (as a String) within colwidth filling in the right
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the right.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * left justified.
   * @param pad The pad character to fill any positions to the right of the
   * given argument, if needed.
   * @return left justified arg
   */
  public static String left(final double arg, final int colwidth, final char pad) {
    return left(Double.toString(arg), colwidth, pad);
  }

  //RIGHT

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with space characters, if needed. If arg (as a String) is longer than
   * the colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @return right justified arg
   */
  public static String right(final String arg, final int colwidth) {
    return right(arg, colwidth, ' ');
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @param pad The pad character to fill any positions to the left of the
   * given argument, if needed.
   * @return right justified arg
   */
  public static String right(final String arg, final int colwidth, final char pad) {
    if (colwidth < 1) { return ""; }
    final String s = (arg == null) ? NULL : arg;
    final int slen = s.length();
    final char[] buf = new char[colwidth];
    int pads = colwidth - slen;
    if (pads > 0) {
      s.getChars(0, slen, buf, pads);
      for (int i = pads; i-- > 0;) {
        buf[i] = pad;
      }
    }
    else { //string is longer than colwidth
      pads = -pads;
      s.getChars(pads, slen, buf, 0);
    }
    return new String(buf);
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with space characters, if needed. If arg (as a String) is longer than
   * the colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @return right justified arg
   */
  public static String right(final long arg, final int colwidth) {
    return right(Long.toString(arg), colwidth, ' ');
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @param pad The pad character to fill any positions to the left of the
   * given argument, if needed.
   * @return right justified arg
   */
  public static String right(final long arg, final int colwidth, final char pad) {
    return right(Long.toString(arg), colwidth, pad);
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with space characters, if needed. If arg (as a String) is longer than
   * the colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @return right justified arg
   */
  public static String right(final int arg, final int colwidth) {
    return right(Integer.toString(arg), colwidth, ' ');
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @param pad The pad character to fill any positions to the left of the
   * given argument, if needed.
   * @return right justified arg
   */
  public static String right(final int arg, final int colwidth, final char pad) {
    return right(Integer.toString(arg), colwidth, pad);
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with space characters, if needed. If arg (as a String) is longer than
   * the colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @return right justified arg
   */
  public static String right(final short arg, final int colwidth) {
    return right(Short.toString(arg), colwidth, ' ');
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @param pad The pad character to fill any positions to the left of the
   * given argument, if needed.
   * @return right justified arg
   */
  public static String right(final short arg, final int colwidth, final char pad) {
    return right(Short.toString(arg), colwidth, pad);
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with space characters, if needed. If arg (as a String) is longer than
   * the colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @return right justified arg
   */
  public static String right(final byte arg, final int colwidth) {
    return right(Byte.toString(arg), colwidth, ' ');
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @param pad The pad character to fill any positions to the left of the
   * given argument, if needed.
   * @return right justified arg
   */
  public static String right(final byte arg, final int colwidth, final char pad) {
    return right(Byte.toString(arg), colwidth, pad);
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with space characters, if needed. If arg (as a String) is longer than
   * the colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @return right justified arg
   */
  public static String right(final boolean arg, final int colwidth) {
    return right(Boolean.toString(arg), colwidth, ' ');
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @param pad The pad character to fill any positions to the left of the
   * given argument, if needed.
   * @return right justified arg
   */
  public static String right(final boolean arg, final int colwidth, final char pad) {
    return right(Boolean.toString(arg), colwidth, pad);
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with space characters, if needed. If arg (as a String) is longer than
   * the colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @return right justified arg
   */
  public static String right(final double arg, final int colwidth) {
    return right(Double.toString(arg), colwidth, ' ');
  }

  /**
   * Right justifies arg (as a String) within colwidth filling in the left
   * with the pad character, if needed. If arg (as a String) is longer than
   * colwidth, it truncates on the left.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given argument is
   * right justified.
   * @param pad The pad character to fill any positions to the left of the
   * given argument, if needed.
   * @return right justified arg
   */
  public static String right(final double arg, final int colwidth, final char pad) {
    return right(Double.toString(arg), colwidth, pad);
  }

  //CENTER

  /**
   * Centers arg (as a String) within colwidth filling in the sides with
   * space characters, if needed.  If arg (as a String) is longer than
   * colwidth, it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @return centered arg
   */
  public static String center(final String arg, final int colwidth) {
    return center(arg, colwidth, ' ');
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with the
   * pad character, if needed. If arg (as a String) is longer than colwidth,
   * it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @param pad The pad character to fill any positions to the sides of the
   * given argument (as a String), if needed.
   * @return centered arg
   */
  public static String center(final String arg, final int colwidth, final char pad) {
    if (colwidth < 1) { return ""; }
    final String s = (arg == null) ? NULL : arg;
    final int slen = s.length();
    final char[] buf = new char[colwidth];
    int pads = colwidth - slen;
    if (pads > 0) {
      for (int i = colwidth; i-- > 0;) { buf[i] = pad; }
      s.getChars(0, slen, buf, pads / 2);
    }
    else {
      pads = (-pads) / 2;
      s.getChars(pads, pads + colwidth, buf, 0);
    }
    return new String(buf);
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with
   * space characters, if needed.  If arg (as a String) is longer than
   * colwidth, it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @return centered arg
   */
  public static String center(final long arg, final int colwidth) {
    return center(Long.toString(arg), colwidth, ' ');
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with the
   * pad character, if needed. If arg (as a String) is longer than colwidth,
   * it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @param pad The pad character to fill any positions to the sides of the
   * given argument (as a String), if needed.
   * @return centered arg
   */
  public static String center(final long arg, final int colwidth, final char pad) {
    return center(Long.toString(arg), colwidth, pad);
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with
   * space characters, if needed.  If arg (as a String) is longer than
   * colwidth, it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @return centered arg
   */
  public static String center(final int arg, final int colwidth) {
    return center(Integer.toString(arg), colwidth, ' ');
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with the
   * pad character, if needed. If arg (as a String) is longer than colwidth,
   * it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @param pad The pad character to fill any positions to the sides of the
   * given argument (as a String), if needed.
   * @return centered arg
   */
  public static String center(final int arg, final int colwidth, final char pad) {
    return center(Integer.toString(arg), colwidth, pad);
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with
   * space characters, if needed.  If arg (as a String) is longer than
   * colwidth, it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @return centered arg
   */
  public static String center(final short arg, final int colwidth) {
    return center(Short.toString(arg), colwidth, ' ');
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with the
   * pad character, if needed. If arg (as a String) is longer than colwidth,
   * it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @param pad The pad character to fill any positions to the sides of the
   * given argument (as a String), if needed.
   * @return centered arg
   */
  public static String center(final short arg, final int colwidth, final char pad) {
    return center(Short.toString(arg), colwidth, pad);
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with
   * space characters, if needed.  If arg (as a String) is longer than
   * colwidth, it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @return centered arg
   */
  public static String center(final byte arg, final int colwidth) {
    return center(Byte.toString(arg), colwidth, ' ');
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with the
   * pad character, if needed. If arg (as a String) is longer than colwidth,
   * it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @param pad The pad character to fill any positions to the sides of the
   * given argument (as a String), if needed.
   * @return centered arg
   */
  public static String center(final byte arg, final int colwidth, final char pad) {
    return center(Byte.toString(arg), colwidth, pad);
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with
   * space characters, if needed.  If arg (as a String) is longer than
   * colwidth, it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @return centered arg
   */
  public static String center(final boolean arg, final int colwidth) {
    return center(Boolean.toString(arg), colwidth, ' ');
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with the
   * pad character, if needed. If arg (as a String) is longer than colwidth,
   * it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @param pad The pad character to fill any positions to the sides of the
   * given argument (as a String), if needed.
   * @return centered arg
   */
  public static String center(final boolean arg, final int colwidth, final char pad) {
    return center(Boolean.toString(arg), colwidth, pad);
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with
   * space characters, if needed.  If arg (as a String) is longer than
   * colwidth, it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @return centered arg
   */
  public static String center(final double arg, final int colwidth) {
    return center(Double.toString(arg), colwidth, ' ');
  }

  /**
   * Centers arg (as a String) within colwidth filling in the sides with the
   * pad character, if needed. If arg (as a String) is longer than colwidth,
   * it truncates on both sides.
   *
   * @param arg The given argument.
   * @param colwidth The width of the space into which the given
   * argument (as a String) is centered.
   * @param pad The pad character to fill any positions to the sides of the
   * given argument (as a String), if needed.
   * @return centered arg
   */
  public static String center(final double arg, final int colwidth, final char pad) {
    return center(Double.toString(arg), colwidth, pad);
  }

  /**
   * Example and test.
   * @param args input parameters
   */
  public static void main(final String[] args) {
    final String s = "This is a string";
    final String left = "->|";
    final String right = "|<-";
    final String out = left + center(s,14) + right;
    println(out);
  }

  private final static boolean enablePrinting = true;

  /**
   * @param format the format
   * @param args the args
   */
  @SuppressWarnings("unused")
  private static final void printf(final String format, final Object ... args) {
    if (enablePrinting) { System.out.printf(format, args); }
  }

  /**
   * @param o the Object to println
   */
  private static final void println(final Object o) {
    if (enablePrinting) { println(o.toString()); }
  }

}
