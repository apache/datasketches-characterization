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
 * This SpecialRandom generator extends the java.util.Random to include some important missing methods.
 */
public class SpecialRandom extends java.util.Random {
  /** use serialVersionUID from JDK 1.1 for interoperability */
  static final long serialVersionUID = 3905348978240129619L;

  /**
   * Constructs a new special random generator.
   */
  public SpecialRandom() {
    super();
  }

  /**
   * Provides a seed to make this generator deterministic.
   * @param seed the given seed.
   */
  public SpecialRandom(final long seed) {
    super(seed);
  }

  /**
   * This makes visible an important hidden class from Random. It provides the next pseudorandom integer value
   * consisting of the number given bits as low order bits.
   * @param bits the number of random low-order bits from the range [1, 32] (inclusive, inclusive)
   * @return the next pseudorandom value consisting of <i>bits</i> low-order bits.
   */
  @Override
  public int next(final int bits) {
    return super.next(bits);
  }

  /**
   * This provides the next pseudorandom long value consisting of the number of given bits as low order bits.
   * @param bits the number of random low-order bits from the range [1, 64] (inclusive, inclusive)
   * @return the next pseudorandom value consisting of <i>bits</i> low-order bits.
   */
  public long nextLong(final int bits) {
    if (bits <= 32) { return next(bits); }
    return ((long)(next(bits - 32)) << 32) + next(32);
  }

  /**
   * Returns an approximate uniform random distribution of long values between 0 (inclusive) and bound (exclusive);
   * @param bound a value from the range [2, bound).
   * @return an approximate uniform random long from the range [2, bound).
   */
  public long nextLong(final long bound) {
    if (bound <= 1L) { throw new IllegalArgumentException("bound must be greater than 1"); }
    final long m = bound - 1L;
    long r;
    if ((bound & m) == 0) { //bound is power of 2
      final int nlz = Long.numberOfLeadingZeros(bound);
      final int bits = 64 - nlz - 1;
      r = nextLong(bits);
    } else {
      r = nextLong(63);
      for (long u = r; u - (r = u % bound) + m < 0; u = nextLong(63)) { }
    }
    return r;
  }

}
