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

/**
 * @author Lee Rhodes
 */
public class GaussianRanks {
  //Fractional ranks computed from the standard normal cumulative distribution.
  public static final double M4SD = 0.0000316712418331; //minus 4 StdDev //0
  public static final double M3SD = 0.0013498980316301; //minus 3 StdDev //1
  public static final double M2SD = 0.0227501319481792; //minus 2 StdDev //2
  public static final double M1SD = 0.1586552539314570; //minus 1 StdDev //3
  public static final double MED  = 0.5; //median                        //4
  public static final double P1SD = 0.8413447460685430; //plus  1 StdDev //5
  public static final double P2SD = 0.9772498680518210; //plus  2 StdDev //6
  public static final double P3SD = 0.9986501019683700; //plus  3 StdDev //7
  public static final double P4SD = 0.9999683287581670; //plus  4 StdDev //8
  public static final double[] FRACTIONS =
    {0.0, M4SD, M3SD, M2SD, M1SD, MED, P1SD, P2SD, P3SD, P4SD, 1.0};
  public static final int FRACT_LEN = FRACTIONS.length;

  public static final double[] FRACTIONS_3SD =
    {0.0, M3SD, M2SD, M1SD, MED, P1SD, P2SD, P3SD, 1.0};
}
