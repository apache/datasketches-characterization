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
	"github.com/apache/datasketches-go/frequencies"
	"math"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

type FrequencyLongSpeedProfile struct {
	config      frequencyJobConfigType
	zipf        *zipfDistribution
	startTime   int64
	stats       frequencySpeedStats
	inputValues []int64
}

type frequencySpeedStats struct {
	buildTimeNs         int64
	updateTimeNs        int64
	serializeTimeNs     int64
	deserializeTimeNs   int64
	numRetainedItems    int64
	serializedSizeBytes int64
}

func NewFrequencyLongSpeedProfile(config frequencyJobConfigType) *FrequencyLongSpeedProfile {
	return &FrequencyLongSpeedProfile{
		config:    config,
		zipf:      newZipfDistribution(int64(config.zipfRange), config.zipfExponent),
		startTime: time.Now().UnixMilli(),
		stats:     frequencySpeedStats{},
	}
}

func (d *FrequencyLongSpeedProfile) run() {
	sb := &strings.Builder{}
	debug.SetGCPercent(-1)
	debug.SetMemoryLimit(math.MaxInt64)

	lgMinStreamLen := d.config.lgMin
	lgMaxStreamLen := d.config.lgMax
	pointsPerOctave := d.config.PPO
	lgMinTrials := d.config.lgMinTrials
	lgMaxTrials := d.config.lgMaxTrials

	minStreamLen := 1 << lgMinStreamLen
	maxStreamLen := 1 << lgMaxStreamLen

	d.setHeader(sb)
	fmt.Println(sb.String())

	streamLength := minStreamLen
	for streamLength <= maxStreamLen {
		sb.Reset()
		numTrials := getNumTrials(streamLength, lgMinStreamLen, lgMaxStreamLen, lgMinTrials, lgMaxTrials)
		d.resetStats()
		for i := 0; i < numTrials; i++ {
			d.prepareTrial(streamLength)
			d.process()
		}
		runtime.GC()
		d.getStats(streamLength, numTrials, sb)
		fmt.Println(sb.String())
		streamLength = int(pwr2SeriesNext(pointsPerOctave, uint64(streamLength)))
	}
}

func (d *FrequencyLongSpeedProfile) process() {
	startBuild := time.Now().UnixNano()
	sketch, err := frequencies.NewLongsSketchWithMaxMapSize(d.config.k)
	if err != nil {
		panic(err)
	}
	stopBuild := time.Now().UnixNano()
	d.stats.buildTimeNs += stopBuild - startBuild

	startUpdate := time.Now().UnixNano()
	for i := 0; i < len(d.inputValues); i++ {
		err := sketch.Update(d.inputValues[i])
		if err != nil {
			panic(err)
		}
	}
	stopUpdate := time.Now().UnixNano()
	d.stats.updateTimeNs += stopUpdate - startUpdate

	startSerialize := time.Now().UnixNano()
	bytes := sketch.ToSlice()
	stopSerialize := time.Now().UnixNano()
	d.stats.serializeTimeNs += stopSerialize - startSerialize

	startDeserialize := time.Now().UnixNano()
	_, err = frequencies.NewLongsSketchFromSlice(bytes)
	if err != nil {
		panic(err)
	}
	stopDeserialize := time.Now().UnixNano()
	d.stats.deserializeTimeNs += stopDeserialize - startDeserialize

	d.stats.numRetainedItems += int64(sketch.GetNumActiveItems())
	d.stats.serializedSizeBytes += int64(len(bytes))

}

func (d *FrequencyLongSpeedProfile) setHeader(sb *strings.Builder) {
	sb.WriteString("Stream\tTrials\tBuild\tUpdate\tSer\tDeser\tItems\tstatsSize")
}

func (d *FrequencyLongSpeedProfile) getStats(streamLength int, numTrials int, sb *strings.Builder) {
	sb.WriteString(fmt.Sprintf("%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f",
		streamLength,
		numTrials,
		float64(d.stats.buildTimeNs)/float64(numTrials),
		float64(d.stats.updateTimeNs)/float64(numTrials)/float64(streamLength),
		float64(d.stats.serializeTimeNs)/float64(numTrials),
		float64(d.stats.deserializeTimeNs)/float64(numTrials),
		float64(d.stats.numRetainedItems)/float64(numTrials),
		float64(d.stats.serializedSizeBytes)/float64(numTrials),
	))
}

func (d *FrequencyLongSpeedProfile) resetStats() {
	d.stats.buildTimeNs = 0
	d.stats.updateTimeNs = 0
	d.stats.serializeTimeNs = 0
	d.stats.deserializeTimeNs = 0
	d.stats.numRetainedItems = 0
	d.stats.serializedSizeBytes = 0
}

func (d *FrequencyLongSpeedProfile) prepareTrial(streamLength int) {
	// prepare input data
	d.inputValues = make([]int64, streamLength)
	for i := 0; i < streamLength; i++ {
		d.inputValues[i] = d.zipf.sample()
	}
}

func getNumTrials(x, lgMinX, lgMaxX, lgMinTrials, lgMaxTrials int) int {
	slope := float64(lgMaxTrials-lgMinTrials) / float64(lgMinX-lgMaxX)
	lgX := math.Log(float64(x)) / math.Log(2.0)
	lgTrials := slope*lgX + float64(lgMaxTrials)
	return int(math.Pow(2.0, lgTrials))
}
