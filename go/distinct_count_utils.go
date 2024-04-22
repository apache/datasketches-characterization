/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main

type baseAccuracyStats interface {
}

type DistinctCountAccuracyProfileRunner interface {
	runTrial(stats []baseAccuracyStats, key uint64) uint64
}

const (
	M4SD = 0.0000316712418331 //minus 4 StdDev
	M3SD = 0.0013498980316301 //minus 3 StdDev
	M2SD = 0.0227501319481792 //minus 2 StdDev
	M1SD = 0.1586552539314570 //minus 1 StdDev
	MED  = 0.5                //median
	P1SD = 0.8413447460685430 //plus  1 StdDev
	P2SD = 0.9772498680518210 //plus  2 StdDev
	P3SD = 0.9986501019683700 //plus  3 StdDev
	P4SD = 0.9999683287581670 //plus  4 StdDev
)

var (
	GAUSSIANS_4SD = []float64{0.0, M4SD, M3SD, M2SD, M1SD, MED, P1SD, P2SD, P3SD, P4SD, 1.0}
	GAUSSIANS_3SD = []float64{0.0, M3SD, M2SD, M1SD, MED, P1SD, P2SD, P3SD, 1.0}
)
