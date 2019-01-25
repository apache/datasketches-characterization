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

package com.yahoo.sketches.characterization.quantiles.druidhistogram;

//CHECKSTYLE.OFF: FinalLocalVariable
//CHECKSTYLE.OFF: JavadocMethod
//CHECKSTYLE.OFF: LineLength
//CHECKSTYLE.OFF: OperatorWrap
//CHECKSTYLE.OFF: NonEmptyAtclauseDescription
//CHECKSTYLE.OFF: JavadocParagraph

//import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

public class Histogram
{
  double[] breaks;
  double[] counts; //one less than breaks

  public Histogram(float[] breaks, double[] counts)
  {
    double[] retVal = new double[breaks.length];
    for (int i = 0; i < breaks.length; ++i) {
      retVal[i] = breaks[i];
    }

    this.breaks = retVal;
    this.counts = counts;
  }

  //@JsonProperty
  public double[] getBreaks()
  {
    return breaks;
  }

  //@JsonProperty
  public double[] getCounts()
  {
    return counts;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if ((o == null) || (getClass() != o.getClass())) {
      return false;
    }

    Histogram that = (Histogram) o;

    if (!Arrays.equals(getBreaks(), that.getBreaks())) {
      return false;
    }
    if (!Arrays.equals(getCounts(), that.getCounts())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    int result = (getBreaks() != null ? ArrayUtils.hashCode(getBreaks(), 0, getBreaks().length) : 0);
    result = (31 * result) + (getCounts() != null ? ArrayUtils.hashCode(
        getCounts(),
        0,
        getCounts().length
    ) : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Histogram{" +
           "breaks=" + Arrays.toString(breaks) +
           ", counts=" + Arrays.toString(counts) +
           '}';
  }
}
