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

	"github.com/apache/datasketches-go/theta"
)

type ThetaUnionUpdateSpeedProfile struct {
	config thetaJobConfig

	updateSketches  []*theta.QuickSelectUpdateSketch
	compactSketches []*theta.CompactSketch

	vIn uint64
}

func MustNewThetaUnionUpdateSpeedProfile(cfg thetaJobConfig) *ThetaUnionUpdateSpeedProfile {
	return &ThetaUnionUpdateSpeedProfile{
		config:          cfg,
		updateSketches:  make([]*theta.QuickSelectUpdateSketch, cfg.numSketches),
		compactSketches: make([]*theta.CompactSketch, cfg.numSketches),
	}
}

func (p *ThetaUnionUpdateSpeedProfile) run() {
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

func (p *ThetaUnionUpdateSpeedProfile) computeNumTrials(curU uint64) int {
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

func (p *ThetaUnionUpdateSpeedProfile) runTrial(uPerTrial int) float64 {
	for i := 0; i < p.config.numSketches; i++ {
		sketch, _ := theta.NewQuickSelectUpdateSketch(
			theta.WithUpdateSketchLgK(p.config.lgK),
		)
		p.updateSketches[i] = sketch
	}

	for u, i := uPerTrial, 0; u > 0; u-- {
		p.vIn++
		p.updateSketches[i].UpdateUint64(p.vIn)

		if i == p.config.numSketches-1 {
			i = 0
		} else {
			i++
		}
	}

	for i, sketch := range p.updateSketches {
		sketch.Trim()
		p.compactSketches[i] = sketch.CompactOrdered()
	}

	union, _ := theta.NewUnion(
		theta.WithUnionLgK(p.config.lgK),
	)

	startTime := time.Now()

	for _, sketch := range p.compactSketches {
		union.Update(sketch)
	}

	elapsedTime := time.Now().Sub(startTime)

	return float64(elapsedTime)
}

func (p *ThetaUnionUpdateSpeedProfile) setHeader(sb *strings.Builder) string {
	sb.WriteString("InU")
	sb.WriteString("\t")
	sb.WriteString("Trials")
	sb.WriteString("\t")
	sb.WriteString("nS/Set")
	return sb.String()
}

func (p *ThetaUnionUpdateSpeedProfile) process(
	meanUpdateTimePerSetNanoSec float64, trials int, uPerTrial uint64, sb *strings.Builder,
) {
	sb.WriteString(fmt.Sprintf("%d", uPerTrial))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", trials))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%e", meanUpdateTimePerSetNanoSec))
}
