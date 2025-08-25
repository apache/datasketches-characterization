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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class LineReader {
  // private static final String LS = System.getProperty("line.separator");
  private int lineNo = 0;
  private String inFile = null;
  private BufferedReader bufReaderIn;

  /**
   * Constructor.
   *
   * @param inFile The file to be read.
   */
  public LineReader(final String inFile) {
    if ((inFile == null) || inFile.isEmpty()) {
      throw new IllegalArgumentException("Input file is null or empty.");
    }
    this.inFile = inFile;
    try {
      bufReaderIn = new BufferedReader(new FileReader(inFile));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Reads lines from a file, which is then processed via a callback to the
   * client. This can be recalled multiple times if <i>lines</i> is given value
   * greater than zero.
   *
   * @param lines The number of lines to be read. Only non-empty lines will be
   * processed by processLine. If it is zero or negative, all lines to the
   * end-of-file will be read.
   * @param processLine The call-back procedure.
   * @return true if 1 or more valid lines were processed, otherwise false.
   */
  public boolean read(final int lines, final ProcessLine processLine) {
    boolean ret = false;
    String line;
    int ctr = 0;
    try {
      while ((line = bufReaderIn.readLine()) != null) {
        lineNo++; // external, file line number, starts with 1
        ctr++;    // internal, used for completion
        if (line.length() == 0) {
          continue;
        }
        // Callback
        processLine.process(line, lineNo);
        ret = true;
        if (lines <= 0) {
          continue;
        } else if (ctr >= lines) {
          break;
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    return ret;
  }

  public String getFileName() {
    return inFile;
  }

  public int getLastLineNumberRead() {
    return lineNo;
  }

}
