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

import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;

/**
 * @author Lee Rhodes
 */
public class RunJob {

  public void task1() {
    final UpdateSketch sk = new UpdateSketchBuilder().build();
    for (int i = 0; i < 2000; i++) { sk.update(i); }
    println("Estimate: " + sk.getEstimate());
  }

  static void println(final Object o) { System.out.println(o.toString()); }

  public static void main(final String[] args) {
    final RunJob runJob = new RunJob();
    runJob.task1();
  }

}
