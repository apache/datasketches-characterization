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
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/apache/datasketches-go/tuple"
)

type ArrayOfNumbersSketchUpdateSpeedProfile struct {
	config arrayOfNumbersSketchJobConfig

	sketch *tuple.ArrayOfNumbersUpdateSketch[float64]
	vIn    uint64
}

func MustNewArrayOfNumberUpdateSpeedProfile(cfg arrayOfNumbersSketchJobConfig) *ArrayOfNumbersSketchUpdateSpeedProfile {
	sketch, err := tuple.NewArrayOfNumbersUpdateSketch[float64](
		cfg.numValues,
		tuple.WithUpdateSketchLgK(cfg.lgK),
		tuple.WithUpdateSketchP(cfg.p),
		tuple.WithUpdateSketchResizeFactor(cfg.resizeFactor),
	)
	if err != nil {
		panic(err)
	}

	return &ArrayOfNumbersSketchUpdateSpeedProfile{
		config: cfg,
		sketch: sketch,
	}
}

func (p *ArrayOfNumbersSketchUpdateSpeedProfile) run() {
	debug.SetMemoryLimit(math.MaxInt64)

	maxU := uint64(1 << p.config.lgMaxU)
	minU := uint64(1 << p.config.lgMinU)
	lastU := uint64(0)

	sb := &strings.Builder{}
	p.setHeader(sb)
	fmt.Println(sb.String())
	sb.Reset()
	for lastU < maxU {
		nextU := pwr2SeriesNext(p.config.uppo, lastU)
		if lastU == 0 {
			nextU = minU
		}

		lastU = nextU

		trials := p.computeNumTrials(nextU)

		runtime.GC()

		totalUpdateTimePerUNanoSec := 0.0
		for t := 0; t < trials; t++ {
			totalUpdateTimePerUNanoSec += p.runTrial(int(nextU))
		}
		meanUpdateTimePerUNanoSec := totalUpdateTimePerUNanoSec / float64(trials)

		p.process(
			meanUpdateTimePerUNanoSec, trials, nextU, sb,
		)

		fmt.Println(sb.String())
		sb.Reset()
	}
}

func (p *ArrayOfNumbersSketchUpdateSpeedProfile) computeNumTrials(curU uint64) int {
	minBpU := uint64(1 << p.config.lgMinBpU)
	maxBpU := uint64(1 << p.config.lgMaxBpU)
	maxT := 1 << p.config.lgMaxT
	minT := 1 << p.config.lgMinT
	if p.config.lgMinT == p.config.lgMaxT || curU <= minBpU {
		return maxT
	}
	if curU >= maxBpU {
		return minT
	}
	lgCurU := math.Log(float64(curU)) / math.Log(2.0)
	slope := float64(p.config.lgMaxT-p.config.lgMinT) / float64(p.config.lgMinBpU-p.config.lgMaxBpU)
	lgTrials := slope*(lgCurU-float64(p.config.lgMinBpU)) + float64(p.config.lgMaxT)
	return int(math.Pow(2.0, lgTrials))
}

func (p *ArrayOfNumbersSketchUpdateSpeedProfile) runTrial(uPerTrial int) float64 {
	p.sketch.Reset()

	values := make([]float64, p.config.numValues)

	startTime := time.Now()

	for u := uPerTrial; u > 0; u-- {
		p.vIn++
		p.sketch.UpdateUint64(p.vIn, values)
	}

	elapsedTime := time.Now().Sub(startTime)

	return float64(elapsedTime) / float64(uPerTrial)
}

func (p *ArrayOfNumbersSketchUpdateSpeedProfile) setHeader(sb *strings.Builder) string {
	sb.WriteString("InU")
	sb.WriteString("\t")
	sb.WriteString("Trials")
	sb.WriteString("\t")
	sb.WriteString("nS/Set")
	return sb.String()
}

func (p *ArrayOfNumbersSketchUpdateSpeedProfile) process(
	meanUpdateTimePerSetNanoSec float64, trials int, uPerTrial uint64, sb *strings.Builder,
) {
	sb.WriteString(fmt.Sprintf("%d", uPerTrial))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", trials))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%e", meanUpdateTimePerSetNanoSec))
}
