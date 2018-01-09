/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization;

import static java.lang.Math.log;

/**
 *
 * @author Lee Rhodes
 */
public interface JobProfile {
  static char TAB = '\t';
  static double LN2 = log(2.0);
  static String LS = System.getProperty("line.separator");

  /**
   * Starts the Job Profile given the job.
   * @param job the given job
   */
  void start(Job job);

  /**
   * For sending output to a file and/or to stdOut.
   * @param s the string to output
   */
  void println(String s);

}
