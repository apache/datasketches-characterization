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

package com.yahoo.sketches.characterization.uniquecount.druidhll;

import java.nio.ByteBuffer;

public class VersionOneHyperLogLogCollector extends HyperLogLogCollector {
  /**
   * Header:
   * Byte 0: version
   * Byte 1: registerOffset
   * Byte 2-3: numNonZeroRegisters
   * Byte 4: maxOverflowValue
   * Byte 5-6: maxOverflowRegister
   */
  public static final byte VERSION = 0x1;
  public static final int REGISTER_OFFSET_BYTE = 1;
  public static final int NUM_NON_ZERO_REGISTERS_BYTE = 2; //and 3
  public static final int MAX_OVERFLOW_VALUE_BYTE = 4; //the exception value
  public static final int MAX_OVERFLOW_REGISTER_BYTE = 5; //and 6, the exception index register
  public static final int HEADER_NUM_BYTES = 7;
  public static final int NUM_BYTES_FOR_DENSE_STORAGE =
      NUM_BYTES_FOR_BUCKETS + HEADER_NUM_BYTES; //1024 + 7 = 1031

  private static final ByteBuffer defaultStorageBuffer = //initial header is 1 in first of 7 bytes
      ByteBuffer.wrap(new byte[]{VERSION, 0, 0, 0, 0, 0, 0}).asReadOnlyBuffer();

  VersionOneHyperLogLogCollector() {
    super(defaultStorageBuffer.duplicate());
  }

  VersionOneHyperLogLogCollector(final ByteBuffer buffer) {
    super(buffer);
  }

  @Override
  public byte getVersion() {
    return VERSION;
  }

  @Override
  public void setVersion(final ByteBuffer buffer) {
    buffer.put(buffer.position(), VERSION);
  }

  @Override
  public byte getRegisterOffset() {
    return getStorageBuffer().get(getInitPosition() + REGISTER_OFFSET_BYTE);
  }

  @Override
  public void setRegisterOffset(final byte registerOffset) {
    getStorageBuffer().put(getInitPosition() + REGISTER_OFFSET_BYTE, registerOffset);
  }

  @Override
  public void setRegisterOffset(final ByteBuffer buffer, final byte registerOffset) {
    buffer.put(buffer.position() + REGISTER_OFFSET_BYTE, registerOffset);
  }

  @Override
  public short getNumNonZeroRegisters() {
    return getStorageBuffer().getShort(getInitPosition() + NUM_NON_ZERO_REGISTERS_BYTE);
  }

  @Override
  public void setNumNonZeroRegisters(final short numNonZeroRegisters) {
    getStorageBuffer().putShort(getInitPosition() + NUM_NON_ZERO_REGISTERS_BYTE, numNonZeroRegisters);
  }

  @Override
  public void setNumNonZeroRegisters(final ByteBuffer buffer, final short numNonZeroRegisters) {
    buffer.putShort(buffer.position() + NUM_NON_ZERO_REGISTERS_BYTE, numNonZeroRegisters);
  }

  @Override
  public byte getMaxOverflowValue() {
    return getStorageBuffer().get(getInitPosition() + MAX_OVERFLOW_VALUE_BYTE);
  }

  @Override
  public void setMaxOverflowValue(final byte value) {
    getStorageBuffer().put(getInitPosition() + MAX_OVERFLOW_VALUE_BYTE, value);
  }

  @Override
  public void setMaxOverflowValue(final ByteBuffer buffer, final byte value) {
    buffer.put(buffer.position() + MAX_OVERFLOW_VALUE_BYTE, value);
  }

  @Override
  public short getMaxOverflowRegister() {
    return getStorageBuffer().getShort(getInitPosition() + MAX_OVERFLOW_REGISTER_BYTE);
  }

  @Override
  public void setMaxOverflowRegister(final short register) {
    getStorageBuffer().putShort(getInitPosition() + MAX_OVERFLOW_REGISTER_BYTE, register);
  }

  @Override
  public void setMaxOverflowRegister(final ByteBuffer buffer, final short register) {
    buffer.putShort(buffer.position() + MAX_OVERFLOW_REGISTER_BYTE, register);
  }

  @Override
  public int getNumHeaderBytes() {
    return HEADER_NUM_BYTES;
  }

  @Override
  public int getNumBytesForDenseStorage() {
    return NUM_BYTES_FOR_DENSE_STORAGE;
  }

  @Override
  public int getPayloadBytePosition() {
    return getInitPosition() + HEADER_NUM_BYTES;
  }

  @Override
  public int getPayloadBytePosition(final ByteBuffer buffer) {
    return buffer.position() + HEADER_NUM_BYTES;
  }
}
