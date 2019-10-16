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

import static org.apache.datasketches.Files.append;
import static org.apache.datasketches.Files.getExistingFile;
import static org.apache.datasketches.Files.getMappedByteBuffer;
import static org.apache.datasketches.Files.openRandomAccessFile;
import static org.apache.datasketches.Files.readLine;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * ArrayList backed text file.
 *
 * @author Lee Rhodes
 */
public class ArrayListFile extends ArrayList<String> {
  private static final long serialVersionUID = 1L;
  private static final String LS = System.getProperty("line.separator");

  // ArrayList constructor wrappers
  /**
   * Constructs an empty list with an initial capacity of ten.
   */
  public ArrayListFile() {
    super();
  }

  /**
   * Constructs a list containing the elements of the specified collection, in
   * the order they are returned by the collection's iterator.
   *
   * @param coll the collection whose elements are to be placed into this list
   * @throws NullPointerException if the specified collection is null
   */
  public ArrayListFile(final Collection<String> coll) {
    super(coll);
  }

  /**
   * Constructs this class with the given initialCapacity.
   *
   * @param initialCapacity the given capacity
   */
  public ArrayListFile(final int initialCapacity) {
    super(initialCapacity);
  }

  // Unique methods
  /**
   * Gets the line of the file from the given index.
   *
   * @param index corresponds to the line number of the text file.
   * @return the line as a string.
   */
  public String getLine(final int index) {
    return get(index);
  }

  /**
   * Adds the given file, specified by the filename, line by line into this
   * ArrayList.
   *
   * @param fileName a fully qualified filename.
   */
  public void addFile(final String fileName) {
    final File file = getExistingFile(fileName);
    final long fileLen = file.length();
    if (fileLen > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "Cannot handle files larger than " + Integer.MAX_VALUE + " bytes.");
    }
    try (RandomAccessFile raf = openRandomAccessFile(file, "r"); // Read only
        FileChannel fc = raf.getChannel();) {
      final MappedByteBuffer mbBuf = getMappedByteBuffer(fc, FileChannel.MapMode.READ_ONLY);
      String s;
      final ByteArrayBuilder bab = new ByteArrayBuilder();
      while ((s = readLine(mbBuf, bab)) != null) {
        add(s);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Adds a multiline string, split into lines, into this ArrayList
   *
   * @param multiline the given multiline
   */
  public void addMultilineString(final String multiline) {
    addAll(Arrays.asList(multiline.split(LS)));
  }

  /**
   * Outputs this ArrayList to the specified fileName.
   *
   * @param fileName the given fileName
   */
  public void toFile(final String fileName) {
    final File file = new File(fileName);
    if (file.exists()) {
      file.delete();
    }
    try (RandomAccessFile raf = openRandomAccessFile(file, "rw");
        FileChannel fc = raf.getChannel();) {
      final int size = size();
      for (int i = 0; i < size; i++) {
        append(get(i), fc);
        append(LS, fc);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
