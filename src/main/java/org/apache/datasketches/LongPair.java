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

public class LongPair implements Comparator<LongPair> {
  public long x;
  public long y;

  public LongPair(final long x, final long y) {
    this.x = x;
    this.y = y;
  }

  @Override
  public int compare(final LongPair p1, final LongPair p2) {
    return (p1.x < p2.x) ? -1 : (p1.x > p2.x) ? 1 : 0;
  }
}
