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
	"fmt"
	"github.com/apache/datasketches-go/hll"
	"math"
	"runtime/debug"
	"strings"
	"time"
)

type DistinctCountSerDeProfile struct {
	config    distinctCountJobConfigType
	sketch    hll.HllSketch
	startTime int64
}

type distinctCountSerdeStats struct {
	serializeTime_nS   int64
	deserializeTime_nS int64
	estimationTime_nS  int64
	size_bytes         uint64
}

func NewDistinctCountSerDeProfile(config distinctCountJobConfigType, tgtType hll.TgtHllType) *DistinctCountSerDeProfile {
	sketch, _ := hll.NewHllSketch(config.lgK, tgtType)
	return &DistinctCountSerDeProfile{
		config:    config,
		sketch:    sketch,
		startTime: time.Now().UnixMilli(),
	}
}

func (d *DistinctCountSerDeProfile) run() {
	var (
		sb        = &strings.Builder{}
		vIn       = int64(0)
		rawStats  = &distinctCountSerdeStats{}
		meanStats = &distinctCountSerdeStats{}
		maxU      = 1 << d.config.lgMaxU
		minU      = 1 << d.config.lgMinU
		lastU     = 0
	)
	debug.SetGCPercent(-1)
	debug.SetMemoryLimit(math.MaxInt64)

	d.setHeader(sb)
	fmt.Println(sb.String())
	sb.Reset()

	for lastU < maxU {
		nextU := minU
		if lastU != 0 {
			nextU = int(pwr2SeriesNext(d.config.uppo, uint64(lastU)))
		}
		lastU = nextU

		sumStats := &distinctCountSerdeStats{}
		trials := d.getNumTrials(nextU)
		for t := 0; t < trials; t++ {
			vIn = d.runTrial(rawStats, vIn, nextU)
			sumStats.add(rawStats)
		}
		meanStats.makeMeanOf(sumStats, trials)
		d.process(meanStats, trials, nextU, sb)
		fmt.Println(sb.String())
		sb.Reset()
	}

}

func (d *DistinctCountSerDeProfile) setHeader(sb *strings.Builder) string {
	sb.WriteString("TrueU")
	sb.WriteString("\t")
	sb.WriteString("Trials")
	sb.WriteString("\t")
	sb.WriteString("Ser_nS")
	sb.WriteString("\t")
	sb.WriteString("DeSer_nS")
	sb.WriteString("\t")
	sb.WriteString("Est_nS")
	sb.WriteString("\t")
	sb.WriteString("Size_B")
	return sb.String()
}

func (d *DistinctCountSerDeProfile) runTrial(stats *distinctCountSerdeStats, key int64, lgDeltaU int) int64 {
	var (
		startEstimationTime_Ns int64
		stopEstimationTime_Ns  int64
		startSerTime_Ns        int64
		stopSerTime_Ns         int64
		startDeserTime_Ns      int64
		stopDeserTime_Ns       int64
		sketchBytes            []byte
		est1                   float64
		est2                   float64
	)
	d.sketch.Reset()

	for u := lgDeltaU; u > 0; u-- {
		key++
		d.sketch.UpdateInt64(key)
	}

	startEstimationTime_Ns = time.Now().UnixNano()
	est1, err := d.sketch.GetEstimate()
	if err != nil {
		panic(err)
	}
	stopEstimationTime_Ns = time.Now().UnixNano()

	if d.config.compact {
		startSerTime_Ns = time.Now().UnixNano()
		sketchBytes, err = d.sketch.ToCompactSlice()
		stopSerTime_Ns = time.Now().UnixNano()
	} else {
		startSerTime_Ns = time.Now().UnixNano()
		sketchBytes, err = d.sketch.ToUpdatableSlice()
		stopSerTime_Ns = time.Now().UnixNano()
	}

	startDeserTime_Ns = time.Now().UnixNano()
	sketchRebuild, err := hll.NewHllSketchFromSlice(sketchBytes, true)
	stopDeserTime_Ns = time.Now().UnixNano()
	if err != nil {
		panic(err)
	}

	est2, err = sketchRebuild.GetEstimate()
	if err != nil {
		panic(err)
	}

	if est1 != est2 {
		panic("Estimation mismatch")
	}

	stats.serializeTime_nS = stopSerTime_Ns - startSerTime_Ns
	stats.deserializeTime_nS = stopDeserTime_Ns - startDeserTime_Ns
	stats.estimationTime_nS = stopEstimationTime_Ns - startEstimationTime_Ns
	stats.size_bytes = uint64(len(sketchBytes))
	return key
}

func (d *DistinctCountSerDeProfile) process(stats *distinctCountSerdeStats, trials int, uPerTrial int, sb *strings.Builder) string {
	sb.WriteString(fmt.Sprintf("%d", uPerTrial))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", trials))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", stats.serializeTime_nS))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", stats.deserializeTime_nS))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", stats.estimationTime_nS))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", stats.size_bytes))
	return sb.String()
}

// getNumTrials computes the number of trials for a given current number of uniques for a
// trial set. This is used in speed trials and decreases the number of trials
// as the number of uniques increase.
func (d *DistinctCountSerDeProfile) getNumTrials(curU int) int {
	minBpU := 1 << d.config.lgMinBpU
	maxBpU := 1 << d.config.lgMaxBpU
	maxT := 1 << d.config.lgMaxT
	minT := 1 << d.config.lgMinT
	if d.config.lgMinT == d.config.lgMaxT || curU <= minBpU {
		return maxT
	}
	if curU >= maxBpU {
		return minT
	}
	lgCurU := math.Log2(float64(curU))
	slope := float64(d.config.lgMaxT-d.config.lgMinT) / float64(d.config.lgMinBpU-d.config.lgMaxBpU)
	lgTrials := slope*(lgCurU-float64(d.config.lgMinBpU)) + float64(d.config.lgMaxT)
	return int(math.Pow(2.0, lgTrials))
}

func (s *distinctCountSerdeStats) add(o *distinctCountSerdeStats) {
	s.serializeTime_nS += o.serializeTime_nS
	s.deserializeTime_nS += o.deserializeTime_nS
	s.estimationTime_nS += o.estimationTime_nS
	s.size_bytes += o.size_bytes
}

func (s *distinctCountSerdeStats) makeMeanOf(o *distinctCountSerdeStats, count int) {
	s.serializeTime_nS = int64(float64(o.serializeTime_nS) / float64(count))
	s.deserializeTime_nS = int64(float64(o.deserializeTime_nS) / float64(count))
	s.estimationTime_nS = int64(float64(o.estimationTime_nS) / float64(count))
	s.size_bytes = o.size_bytes / uint64(count)
}
