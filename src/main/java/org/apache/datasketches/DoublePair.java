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

import java.util.Comparator;

/**
 * A pair (double x, double y) with a comparator on x.
 */
public class DoublePair implements Comparator<DoublePair> {
  public double x;
  public double y;

  public DoublePair(final double x, final double y) {
    this.x = x;
    this.y = y;
  }

  @Override
  public int compare(final DoublePair p1, final DoublePair p2) {
    return Double.compare(p1.x, p2.x); // handles NaN, +/- 0, etc.
  }

}
