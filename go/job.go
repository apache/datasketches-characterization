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

import "math"

type JobProfile interface {
	run()
}

// countPoints Counts the actual number of plotting points between lgStart and lgEnd assuming the given PPO.
// This is not a simple linear function due to points that may be skipped in the low range.
// param lgStart Log2 of the starting value
// param lgEnd Log2 of the ending value
// param ppo the number of logarithmically evenly spaced points per octave.
// returns the actual number of plotting points between lgStart and lgEnd.
func countPoints(lgStart, lgEnd, ppo int) int {
	p := uint64(1) << lgStart
	end := uint64(1) << lgEnd
	count := 0
	for p <= end {
		p = pwr2LawNext(ppo, p)
		count++
	}
	return count
}

// pwr2LawNext Computes the next larger integer point in the power series
// point = 2^( i / ppo ) given the current point in the series.
// For illustration, this can be used in a loop as follows:
//
//	int maxP = 1024;
//	int minP = 1;
//	int ppo = 2;
//
//	for (int p = minP; p <= maxP; p = pwr2LawNext(ppo, p)) {
//	  System.out.print(p + " ");
//	}
//	//generates the following series:
//	//1 2 3 4 6 8 11 16 23 32 45 64 91 128 181 256 362 512 724 1024
//
// param ppo Points-Per-Octave, or the number of points per integer powers of 2 in the series.
// param curPoint the current point of the series. Must be &ge; 1.
// returns the next point in the power series.
func pwr2LawNext(ppo int, curPoint uint64) uint64 {
	cur := curPoint
	if cur < 1 {
		cur = 1
	}
	gi := int64(math.Log2(float64(cur)) * float64(ppo))
	var next uint64
	for {
		next = uint64(math.Pow(2.0, float64(gi)/float64(ppo)))
		if next > curPoint {
			break
		}
		gi++
	}
	return next
}
