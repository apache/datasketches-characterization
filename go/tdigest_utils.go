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

import "sort"

type tdigestJobConfig struct {
	lgMin int
	lgMax int
	ppo   int

	numTrials   int
	errorPCT    int
	lgMinTrials int
	lgMaxTrials int

	lgMinStreamLength int
	lgMaxStreamLength int

	k     int
	ranks []float64

	numSketches int
}

func getQuantile(values []float64, rank float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * rank)
	return values[index]
}

func getRankMidpoint(values []float64, value float64) float64 {
	n := len(values)
	if n == 0 {
		return 0
	}
	lower := sort.SearchFloat64s(values, value)
	upper := lower
	for upper < n && values[upper] == value {
		upper++
	}
	return float64(lower+upper) / 2.0 / float64(n)
}
