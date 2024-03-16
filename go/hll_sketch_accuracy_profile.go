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

import (
	"github.com/apache/datasketches-go/hll"
)

type HllSketchAccuracyRunner struct {
	sketch hll.HllSketch
}

func NewHllSketchAccuracyRunner(lgK int, tgtType hll.TgtHllType) *HllSketchAccuracyRunner {
	sketch, _ := hll.NewHllSketch(lgK, tgtType)
	return &HllSketchAccuracyRunner{
		sketch: sketch,
	}
}

func (h *HllSketchAccuracyRunner) runTrial(stats []*accuracyStats, key uint64) uint64 {
	h.sketch.Reset()

	lastUniques := uint64(0)
	for _, stat := range stats {
		delta := stat.trueValue - lastUniques
		for u := uint64(0); u < delta; u++ {
			h.sketch.UpdateUInt64(key)
			key++
		}
		lastUniques += delta
		est, _ := h.sketch.GetEstimate()
		stat.update(est)
	}

	return key
}
