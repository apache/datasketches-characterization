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
	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/hll"
	"github.com/apache/datasketches-go/kll"
)

// HllSketchBoundsAccuracyRunner is A Runner for HLL tracking boundsAccuracyStats
type HllSketchBoundsAccuracyRunner struct {
	sketch hll.HllSketch
}

type boundsAccuracyStats struct {
	qsk       *kll.ItemsSketch[float64]
	sumLB3    float64
	sumLB2    float64
	sumLB1    float64
	sumUB3    float64
	sumUB2    float64
	sumUB1    float64
	trueValue uint64
}

func newBoundsAccuracyStats(k int, trueValue uint64) *boundsAccuracyStats {
	qsk, _ := kll.NewKllItemsSketch[float64](uint16(k), 8, common.ArrayOfDoublesSerDe{})
	return &boundsAccuracyStats{
		qsk:       qsk,
		trueValue: trueValue,
	}
}

func (a *boundsAccuracyStats) update(
	est float64,
	lb3 float64,
	lb2 float64,
	lb1 float64,
	ub1 float64,
	ub2 float64,
	ub3 float64,
) {
	a.qsk.Update(est)
	a.sumLB3 += lb3
	a.sumLB2 += lb2
	a.sumLB1 += lb1
	a.sumUB1 += ub1
	a.sumUB2 += ub2
	a.sumUB3 += ub3
}

func NewHllSketchBoundsAccuracyRunner(lgK int, tgtType hll.TgtHllType) *HllSketchBoundsAccuracyRunner {
	sketch, _ := hll.NewHllSketch(lgK, tgtType)
	return &HllSketchBoundsAccuracyRunner{
		sketch: sketch,
	}
}

func (h *HllSketchBoundsAccuracyRunner) runTrial(stats []baseAccuracyStats, key uint64) uint64 {
	h.sketch.Reset()

	lastUniques := uint64(0)
	for _, ostat := range stats {
		stat := ostat.(*boundsAccuracyStats)
		delta := stat.trueValue - lastUniques
		for u := uint64(0); u < delta; u++ {
			h.sketch.UpdateUInt64(key)
			key++
		}
		lastUniques += delta
		est, _ := h.sketch.GetEstimate()
		lb3, _ := h.sketch.GetLowerBound(3)
		lb2, _ := h.sketch.GetLowerBound(2)
		lb1, _ := h.sketch.GetLowerBound(1)

		ub1, _ := h.sketch.GetUpperBound(1)
		ub2, _ := h.sketch.GetUpperBound(2)
		ub3, _ := h.sketch.GetUpperBound(3)

		stat.update(est, lb3, lb2, lb1, ub1, ub2, ub3)
	}

	return key
}
