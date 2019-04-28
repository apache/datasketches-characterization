/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import static java.lang.Math.log;

/**
 * Note all JobProfiles must have a public default or empty constructor.
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
   * Allows for smooth shutdown of multiple threads
   */
  void shutdown();

  /**
   * Performs any cleanup if necessary after the job completes
   */
  void cleanup();

  /**
   * For sending a string to the configured PrintStream "out", which may be a file
   * and/or to stdOut.
   * A line separator is added at the end.
   * @param s the string to send to the configured PrintStream "out".
   */
  void println(String s);
}
