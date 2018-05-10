/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.yahoo.sketches.characterization.uniquecount.druidhll;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Contains data hashing methods used before applying Hyper Log Log.
 *
 * <p>{@link HyperLogLogCollector#add(byte[])} requires hashed value on input.
 * This class makes it easier to achieve consistent value hashing for both internal aggregation
 * and external hashing for pre-computed aggregation.
 *
 * <p>By default 128-bit murmur3 algorithm, x64 variant is used. Implementation is thread safe.
 *
 * <p>Caution! changing of implementation may cause improper cardinality estimation between
 * data hashed with different versions.
 */
public class HyperLogLogHash {
  public static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;
  private static final HyperLogLogHash DEFAULT = new HyperLogLogHash(Hashing.murmur3_128());

  public static HyperLogLogHash getDefault()
  {
    return DEFAULT;
  }

  private final HashFunction hashFunction;

  public HyperLogLogHash(final HashFunction hashFunction)
  {
    this.hashFunction = hashFunction;
  }

  public byte[] hash(final byte[] rawValue)
  {
    return hashFunction.hashBytes(rawValue).asBytes();
  }

  public byte[] hash(final String rawValue)
  {
    return hash(toUtf8(rawValue));
  }

  static final byte[] toUtf8(final String string) {
      return string.getBytes(UTF8_CHARSET);
  }
}
