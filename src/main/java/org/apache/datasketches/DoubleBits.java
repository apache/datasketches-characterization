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

import org.testng.annotations.Test;

/**
 * If you are developing any functions that operate directly on the IEEE 754
 * double-precision 64-bit fields you will need this class.
 * @author Lee Rhodes
 */
public final class DoubleBits {
  public static final long SIGN = 1L << 63; //only the MSB (sign) set
  public static final long DEXP1 = 1L << 52; //an exponent value of 1
  public static final long DEXP2 = 2L << 52; //an exponent value of 2
  public static final long DMANMASK = DEXP1 - 1L; //mantissa mask (52 1's)
  public static final long DEXP1023 = 1023L << 52; //exp = 1023 (10 1's)
  public static final long DEXP1025 = 1025L << 52; //exp = 1025
  public static final long DEXP1026 = 1026L << 52; //exp = 1026
  public static final long DEXPMASK = 2047L << 52;//exp = 2047 (11 1's), mask.

  private DoubleBits() {
    //Empty Constructor
  }

  /**
   * Returns true if the IEEE 754 sign bit is set even if the value represents a NaN or zero.
   * @param d the given double
   * @return the sign bit.
   */
  public static boolean isNegative(final double d) {
    final long bits = Double.doubleToRawLongBits(d);
    return ((bits & SIGN) != 0L);
  }

  /**
   * Returns true if the value is a +/- denormalized number but not +/- zero.
   * @param d the given double
   * @return true if the value is a +/- denormalized number but not +/- zero.
   */
  public static boolean isDenormal(final double d) {
    final long bits = Double.doubleToRawLongBits(d);
    return (d != 0.0) && ((bits & DEXPMASK) == 0L) ;
  }

  /**
   * Returns true if the value is +/- Infinity
   * @param d the given double
   * @return true if the value is +/- Infinity
   */
  public static boolean isInfinity(final double d) {
    final long bits = Double.doubleToRawLongBits(d);
    return ((bits & ~SIGN) == DEXPMASK);
  }

  /**
   * Returns true if the value is not a NaN or Infinity
   * @param d the given double
   * @return true if the value is not a NaN or Infinity
   */
  public static boolean isValid(final double d) {
    final long bits = Double.doubleToRawLongBits(d);
    return ((bits & DEXPMASK) != DEXPMASK);
  }

  /**
   * Returns true if the value is a valid argument for the log function.
   * In other words, it cannot be +/- NaN, zero, or Infinity, but it may
   * be a denormal number.
   * @param d the given double
   * @return true if the value is a valid argument for the log function
   */
  public static boolean isValidLogArgument(final double d) {
    final long bits = Double.doubleToRawLongBits(d);
    return ((bits + DEXP1) > DEXP1);
  }

  /**
   * Returns true if the value is a denormal or not a valid argument for the log function.
   * @param d the given double
   * @return true if the value is a denormal or not a valid argument for the log function.
   */
  public static boolean isDenormalOrNotValidLogArgument(final double d) {
    final long bits = Double.doubleToRawLongBits(d);
    return ((bits + DEXP1) < DEXP2);
  }

  /**
   * Returns the signum function of the argument; zero if the argument
   * is zero, 1.0 if the argument is greater than zero, -1.0 if the
   * argument is less than zero.  If the argument is +/- NaN or zero,
   * the result is the same as the argument.<br/>
   *
   * <p>Note that this is a faster, simpler replacement for the needlessly
   * complicated implementation in sun.misc.FpUtils.</p>
   *
   * @param d the given double
   * @return the signum function of the argument
   */
  public static double signum(final double d) {
    return ((d != d) || (d == 0.0)) ? d : (d < 0.0) ? -1.0 : 1.0;
  }

  /**
   * Returns the 11 bit exponent of a double flush right as an int.
   * @param d the given double
   * @return the exponent bits.
   */
  public static int exponentToIntBits(final double d) {
    final long bits = Double.doubleToRawLongBits(d);
    return (int)((bits & DEXPMASK) >>> 52);
  }

  /**
   * Returns the 52 bits of the mantissa as a long.
   * @param d the given double
   * @return the mantissa bits.
   */
  public static long mantissaToLongBits(final double d) {
    final long bits = Double.doubleToRawLongBits(d);
    return bits & DMANMASK;
  }

  /**
   * Returns the mathematical Base 2 exponent as an int. The offset
   * has been removed.
   * @param d the given double
   * @return the mathematical Base 2 exponent
   */
  public static int base2Exponent(final double d) {
    final int e = exponentToIntBits(d);
    return (e == 0) ? -1022 : e - 1023;
  }

  /**
   * Given the double value d, replace the exponent bits
   * with the raw exponent value provided in exp.  If you are converting from
   * a mathematical Base 2 exponent, the offset of 1023 must be added first.
   * @param d the given double
   * @param exp the given exponent as raw bits
   * @return the double value d with replaced exponent bits
   */
  public static double setRawExponentBits(final double d, final int exp) {
    long bits = Double.doubleToRawLongBits(d);
    bits &= ~DEXPMASK; //remove old exponent bits
    bits |= (exp & 0x7FFL) << 52;//insert the new exponent
    return Double.longBitsToDouble(bits);
  }

  /**
   * Given the double value d, replace the mantissa bits
   * with the given mantissa.
   * @param d the given double
   * @param man the given mantissa
   * @return the double value d with replaced mantissa bits
   */
  public static double setMantissaBits(final double d, final long man) {
    long bits = Double.doubleToRawLongBits(d);
    bits &= ~DMANMASK; //remove old mantissa bits
    bits |= (DMANMASK & man); //insert the new mantissa
    return Double.longBitsToDouble(bits);
  }

  /**
   * Returns the given double with the sign bit set as specified by the given boolean.
   * @param d the given double
   * @param negative desired value
   * @return the given double with the sign bit set as specified by the given boolean.
   */
  public static double setSignBit(final double d, final boolean negative) {
    final long bits = Double.doubleToRawLongBits(d);
    return negative
        ? Double.longBitsToDouble(bits | SIGN)
            : Double.longBitsToDouble(bits & ~SIGN);
  }

  /**
   * Given a double, this returns a decimal value representing the effective
   * fractional value of the double's mantissa, which is independent of the
   * value of the exponent with only one exception. If the exponent field 'e'
   * of a double is in the range [1, 2046] the value of a double can be
   * expressed mathematically as 1.fraction X 2^(e-1023). If the exponent
   * value e == 0, the value is expressed as 0.fraction X 2^(-1022), which is
   * called a denormal number. Denormal numbers are only required for
   * magnitudes less than 2.2250738585072014E-308. Very small indeed and much
   * smaller than normal rounding errors.
   *
   * @param d the given double
   * @return a value representing the fractional value of the
   *         double's mantissa. It will be in the format 1.fraction or
   *         0.fraction depending on the exponent as explained above.
   */
  public static double mantissaFraction(final double d) {
    final long bits = Double.doubleToRawLongBits(d);
    final long exp = bits & DEXPMASK;
    final long sign = bits & SIGN; //required in case d is a NaN or zero
    //remove exp & sign, insert exponent for 1.0
    final long man = (bits & DMANMASK) | DEXP1023;
    double d2 = Double.longBitsToDouble(man); //back to double as 1.xxxxx
    d2 = (exp == 0L) ? d2 - 1.0 : d2;
    return (sign != 0L) ? -d2 : d2;
  }

  /**
   * Returns the given double as a string of the form
   * <pre>
   *  SP.FFFFF...FFFBTEEEE
   *  Where
   *    S = the sign of the magnitude only if negative, no space if positive
   *    P = mantissa prefix of 1 or 0
   *    F = decimal fraction
   *    B = indicates binary power of 2 representation
   *    T = the sign of the exponent only if negative
   *    E = the exponent of 2
   * </pre>
   * @param d the given double
   * @return the given double as a string
   */
  public static String doubleToBase2String(final double d) {
    final boolean sign = isNegative(d);
    if (d == 0.0) { return sign ? "-0.0B0" : "0.0B0"; }
    return Double.toString(mantissaFraction(d)) + "B" + base2Exponent(d);
  }

  /**
   * Returns a formatted string representing the fields of an
   * IEEE 754 64-bit double-precision floating point value.
   * The output string is in the form:
   * <pre>
   *  S EEEEEEEEEEE MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
   * </pre>
   * where S represents the sign bit, E represents bits of the Exponent field,
   * and M represents bits of the Mantissa field of the double value.
   * @param d the given double
   * @return the formatted string of the fields of a <i>double</i>.
   */
  public static String doubleToBitString(final double d) {
    final long bits = Double.doubleToRawLongBits(d);
    return longBitsToDoubleBitString(bits);
  }

  /**
   * Returns a formatted string representing the fields of an
   * IEEE 754 64-bit double-precision floating point value presented as a
   * <i>long</i>.  The output string is in the form:
   * <pre>
   *  S EEEEEEEEEEE MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM
   * </pre>
   * where S represents the sign bit, E represents bits of the Exponent field,
   * and M represents bits of the Mantissa field of the double value.
   * @param bits the <i>double</i> value in the form of a <i>long</i> to be
   * examined.
   * @return the formatted string of the fields of a <i>double</i>.
   */
  public static String longBitsToDoubleBitString(final long bits) {
    final int intSign = ((bits & SIGN) != 0L) ? 1 : 0;
    final int e = (int)((bits & DEXPMASK) >>> 52);
    final String estr = Align.right(Integer.toBinaryString(e), 11, '0');
    final long m = bits & DMANMASK;
    final String mstr = Align.right(Long.toBinaryString(m), 52, '0');
    return intSign + " " + estr + " " + mstr;
  }

  /**
   * Helper method to determine whether a specified string is a
   * parsable numeric value or not.
   *
   * @param string the input string to analyze.
   *
   * @return true if the value is numeric (integer, float, double); false if
   *         not.
   */
  public static boolean isNumeric(final String string) {

    boolean isNum = false;

    try {

      Double.parseDouble(string);
      isNum = true;

    }  catch (final NumberFormatException exc ) {
      // We have a non numeric value.
    }

    return isNum;
  }

  private static void checkDoubleBits(final double d, final String txt) {
    println("Test     :" + txt);
    println("Value    : " + d);
    println("Bits     : " + doubleToBitString(d));
    println("Base2    : " + doubleToBase2String(d));
    println("Negative : " + isNegative(d));
    println("Denormal : " + isDenormal(d));
    println("Infinity : " + isInfinity(d));
    println("Valid    : " + isValid(d));
    println("Valid Log: " + isValidLogArgument(d));
    println("Signum   : " + signum(d));
    println("");
  }

  @Test
  public static void doubleBitsTest() {
    checkDoubleBits(4.9E-324,"+Denormal");
    checkDoubleBits(-4.9E-324,"-Denormal");
    checkDoubleBits(0.0,"+Zero");
    checkDoubleBits(-0.0,"-Zero");
    checkDoubleBits(1.0,"+1.0");
    checkDoubleBits(-1.0,"-1.0");
    checkDoubleBits(Double.MAX_VALUE,"MAX_VALUE");
    checkDoubleBits(Double.MIN_VALUE,"MIN_VALUE");
    checkDoubleBits(Double.POSITIVE_INFINITY,"+INFINITY");
    checkDoubleBits(Double.NEGATIVE_INFINITY,"-INFINITY");
    checkDoubleBits(Double.NaN,"+QNaN"); //Quiet NaN
    checkDoubleBits(setSignBit(Double.NaN, true),"-QNaN"); //-Quiet NaN
  }

  /**
   * The maximum integral value that a double can resolve exactly is 8PebiBytes
   * or (1L << 53).
   */
  @Test
  public static void checkMaxIntegralPrcisionOfDouble() {
    final double EIGHT_PEBI = (1L << 53);
    println(doubleToBitString(EIGHT_PEBI + 1L) + ", " + Double.toString(EIGHT_PEBI + 1L)); //over the limit
    println(doubleToBitString(EIGHT_PEBI) + ", " + Double.toString(EIGHT_PEBI));
    println(doubleToBitString(EIGHT_PEBI - 1L) + ", " + Double.toString(EIGHT_PEBI - 1L));
  }

  /**
   * The maximum integral value that a float can resolve exactly is 16MebiBytes
   * or (1 << 24).
   */
  @Test
  public static void checkMaxIntegralPrcisionOfFloat() {
    final float SIXTEEN_MEBI = (1 << 24);
    println(Float.toString(SIXTEEN_MEBI + 1)); //over the limit
    println(Float.toString(SIXTEEN_MEBI));
    println(Float.toString(SIXTEEN_MEBI - 1));
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
    if (enablePrinting) { System.out.println(o.toString()); }
  }
}
