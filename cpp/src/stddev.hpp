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

#ifndef STDDEV_HPP_
#define STDDEV_HPP_

namespace datasketches {

const double M3SD = 0.0013498980316301; //minus 3 StdDev
const double M2SD = 0.0227501319481792; //minus 2 StdDev
const double M1SD = 0.1586552539314570; //minus 1 StdDev
const double P1SD = 0.8413447460685430; //plus  1 StdDev
const double P2SD = 0.9772498680518210; //plus  2 StdDev
const double P3SD = 0.9986501019683700; //plus  3 StdDev

}

#endif
