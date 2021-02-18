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

}
