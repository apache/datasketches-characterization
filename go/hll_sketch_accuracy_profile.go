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

import "github.com/apache/datasketches-go/hll"

type HllSketchAccuracyProfile struct {
}

func NewHllSketchAccuracyProfile() *HllSketchAccuracyProfile {
	return &HllSketchAccuracyProfile{}
}

func (HllSketchAccuracyProfile) runTrial(stats []accuracyStats, key uint64) uint64 {
	lgK := 12

	s, _ := hll.NewHllSketch(lgK, hll.TgtHllTypeDefault)
	count := 0

	for _, stat := range stats {
		delta := stat.trueValue - count
		for i := 0; i < delta; i++ {
			s.UpdateUInt64(key)
			key++
		}
		count += delta
		//stat.update(s.get_estimate())
	}

	return key
}
