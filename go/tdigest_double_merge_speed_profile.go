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
	"math"
	"math/rand"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/apache/datasketches-go/tdigest"
)

type TDigestDoubleMergeSpeedProfile struct {
	config tdigestJobConfig
}

func MustNewTDigestDoubleMergeSpeedProfile(cfg tdigestJobConfig) *TDigestDoubleMergeSpeedProfile {
	return &TDigestDoubleMergeSpeedProfile{
		config: cfg,
	}
}

func (p *TDigestDoubleMergeSpeedProfile) run() {
	debug.SetMemoryLimit(math.MaxInt64)

	fmt.Println("Stream\tTrials\tBuild\tUpdate\tMerge\tSize")

	maxStreamLength := uint64(1) << p.config.lgMaxStreamLength
	values := make([]float64, maxStreamLength)
	sketches := make([]*tdigest.Double, p.config.numSketches)

	streamLength := uint64(1) << p.config.lgMinStreamLength
	for streamLength < maxStreamLength {
		numTrials := getNumTrials(
			int(streamLength),
			p.config.lgMin,
			p.config.lgMax,
			p.config.lgMinTrials,
			p.config.lgMaxTrials,
		)

		var (
			totalBuildTimeNS  int64
			totalUpdateTimeNS int64
			totalMergeTimeNS  int64
			totalSizeBytes    int64
		)

		for t := 0; t < numTrials; t++ {
			for i := uint64(0); i < streamLength; i++ {
				values[i] = rand.Float64()
			}

			runtime.GC()

			buildTimeNS, updateTimeNS, mergeTimeNS, sizeBytes := p.runTrial(values[:streamLength], sketches)
			totalBuildTimeNS += buildTimeNS
			totalUpdateTimeNS += updateTimeNS
			totalMergeTimeNS += mergeTimeNS
			totalSizeBytes += sizeBytes
		}

		avgBuildTimeNS := float64(totalBuildTimeNS) / float64(numTrials) / float64(p.config.numSketches)
		avgUpdateTimePerItemNS := float64(totalUpdateTimeNS) / float64(numTrials) / float64(streamLength) / float64(p.config.numSketches)
		avgMergeTimeNS := float64(totalMergeTimeNS) / float64(numTrials) / float64(p.config.numSketches)
		avgSizeBytes := float64(totalSizeBytes) / float64(numTrials) / float64(p.config.numSketches)

		fmt.Printf("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\n",
			streamLength, numTrials, avgBuildTimeNS, avgUpdateTimePerItemNS, avgMergeTimeNS, avgSizeBytes)

		streamLength = pwr2SeriesNext(p.config.ppo, streamLength)
	}
}

func (p *TDigestDoubleMergeSpeedProfile) runTrial(
	values []float64, sketches []*tdigest.Double,
) (buildTimeNS, updateTimeNS, mergeTimeNS, sizeBytes int64) {
	startBuild := time.Now()
	for i := 0; i < p.config.numSketches; i++ {
		sketch, _ := tdigest.NewDouble(uint16(p.config.k))
		sketches[i] = sketch
	}
	buildTimeNS = time.Since(startBuild).Nanoseconds()

	startUpdate := time.Now()
	for _, sketch := range sketches {
		for _, v := range values {
			sketch.Update(v)
		}
	}
	updateTimeNS = time.Since(startUpdate).Nanoseconds()

	merged, _ := tdigest.NewDouble(uint16(p.config.k))
	startMerge := time.Now()
	for i := 0; i < p.config.numSketches; i++ {
		merged.Merge(sketches[i])
	}
	mergeTimeNS = time.Since(startMerge).Nanoseconds()

	serialized, _ := tdigest.EncodeDouble(merged, false)
	sizeBytes = int64(len(serialized))

	return buildTimeNS, updateTimeNS, mergeTimeNS, sizeBytes
}
