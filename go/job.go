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

// countPoints return the actual number of plotting points between lgStart and lgEnd assuming the given PPO
// and a logBase of 2.
// This is not a simple linear function due to points that may be skipped in the low range.
func countPoints(lgStart, lgEnd, ppo int) int {
	p := uint64(1) << lgStart
	end := uint64(1) << lgEnd
	count := 0
	for p <= end {
		p = pwr2SeriesNext(ppo, p)
		count++
	}
	return count
}

func pwr2SeriesNext(ppo int, curPoint uint64) uint64 {
	cur := curPoint
	if cur < 1 {
		cur = 1
	}
	gi := int(math.Round(math.Log2(float64(cur)) * float64(ppo)))

	var next uint64
	for {
		gi++
		next = uint64(math.Round(math.Pow(2.0, float64(gi)/float64(ppo))))
		if next > curPoint {
			break
		}
	}
	return next
}
