/*
 * Copyright 2016, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class LineReader {
  // private static final String LS = System.getProperty("line.separator");
  private int lineNo_ = 0;
  private String inFile_ = null;
  private BufferedReader bufReaderIn_;

  /**
   * Constructor.
   *
   * @param inFile The file to be read.
   */
  public LineReader(final String inFile) {
    if ((inFile == null) || inFile.isEmpty()) {
      throw new IllegalArgumentException("Input file is null or empty.");
    }
    inFile_ = inFile;
    try {
      bufReaderIn_ = new BufferedReader(new FileReader(inFile));
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
      while ((line = bufReaderIn_.readLine()) != null) {
        lineNo_++; // external, file line number, starts with 1
        ctr++; // internal used for completion
        if (line.length() == 0) {
          continue;
        }
        // Callback
        processLine.process(line, lineNo_);
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
    return inFile_;
  }

  public int getLastLineNumberRead() {
    return lineNo_;
  }

}
