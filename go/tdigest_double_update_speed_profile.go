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

type TDigestDoubleUpdateSpeedProfile struct {
	config tdigestJobConfig
}

func MustNewTDigestDoubleUpdateSpeedProfile(cfg tdigestJobConfig) *TDigestDoubleUpdateSpeedProfile {
	return &TDigestDoubleUpdateSpeedProfile{
		config: cfg,
	}
}

func (p *TDigestDoubleUpdateSpeedProfile) run() {
	debug.SetMemoryLimit(math.MaxInt64)

	fmt.Println("Stream\tTrials\tBuild\tUpdate\tSize")

	maxStreamLength := uint64(1) << p.config.lgMaxStreamLength
	values := make([]float64, maxStreamLength)

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
			totalSizeBytes    int64
		)

		for t := 0; t < numTrials; t++ {
			for i := uint64(0); i < streamLength; i++ {
				values[i] = rand.Float64()
			}

			runtime.GC()

			buildTimeNS, updateTimeNS, sizeBytes := p.runTrial(values[:streamLength])
			totalBuildTimeNS += buildTimeNS
			totalUpdateTimeNS += updateTimeNS
			totalSizeBytes += sizeBytes
		}

		avgBuildTimeNS := float64(totalBuildTimeNS) / float64(numTrials)
		avgUpdateTimePerItemNS := float64(totalUpdateTimeNS) / float64(numTrials) / float64(streamLength)
		avgSizeBytes := float64(totalSizeBytes) / float64(numTrials)

		fmt.Printf("%d\t%d\t%.2f\t%.2f\t%.2f\n",
			streamLength, numTrials, avgBuildTimeNS, avgUpdateTimePerItemNS, avgSizeBytes)

		streamLength = pwr2SeriesNext(p.config.ppo, streamLength)
	}
}

func (p *TDigestDoubleUpdateSpeedProfile) runTrial(values []float64) (buildTimeNS, updateTimeNS, sizeBytes int64) {
	startBuild := time.Now()
	sketch, _ := tdigest.NewDouble(uint16(p.config.k))
	buildTimeNS = time.Since(startBuild).Nanoseconds()

	startUpdate := time.Now()
	for _, v := range values {
		sketch.Update(v)
	}
	updateTimeNS = time.Since(startUpdate).Nanoseconds()

	serialized, _ := tdigest.EncodeDouble(sketch, false)
	sizeBytes = int64(len(serialized))

	return buildTimeNS, updateTimeNS, sizeBytes
}
