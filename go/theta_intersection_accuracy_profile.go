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
	"strings"
	"time"

	"github.com/apache/datasketches-go/theta"
)

type ThetaIntersectionAccuracyProfile struct {
	config thetaJobConfig

	smallSketch  *theta.QuickSelectUpdateSketch
	largeSketch  *theta.QuickSelectUpdateSketch
	intersection *theta.Intersection

	stats     []*accuracyStats
	startTime int64
}

func MustNewIntersectionThetaAccuracyProfile(cfg thetaJobConfig) *ThetaIntersectionAccuracyProfile {
	if !cfg.isIntersectionProfile {
		panic("isIntersectionProfile is false")
	}

	smallSketch, err := theta.NewQuickSelectUpdateSketch(
		theta.WithUpdateSketchLgK(cfg.lgK),
		theta.WithUpdateSketchP(cfg.p),
		theta.WithUpdateSketchResizeFactor(cfg.resizeFactor),
	)
	if err != nil {
		panic(err)
	}

	largeSketch, err := theta.NewQuickSelectUpdateSketch(
		theta.WithUpdateSketchLgK(cfg.lgK),
		theta.WithUpdateSketchP(cfg.p),
		theta.WithUpdateSketchResizeFactor(cfg.resizeFactor),
	)
	if err != nil {
		panic(err)
	}

	stats := make([]*accuracyStats, 0)
	baseStats := buildLog2IntersectionAccuracyStatsArray(cfg.lgMinU, cfg.lgMaxU, cfg.uppo, cfg.lgQK)
	for _, stat := range baseStats {
		stats = append(stats, stat)
	}

	return &ThetaIntersectionAccuracyProfile{
		config:       cfg,
		smallSketch:  smallSketch,
		largeSketch:  largeSketch,
		intersection: theta.NewIntersection(),
		stats:        stats,
		startTime:    time.Now().UnixMilli(),
	}
}

func (p *ThetaIntersectionAccuracyProfile) run() {
	minT := 1 << p.config.lgMinT
	maxT := 1 << p.config.lgMaxT
	maxU := 1 << p.config.lgMaxU

	vIn := uint64(0)

	// This will generate a table of data for each intermediate Trials point
	lastTpt := 0
	for lastTpt < maxT {
		nextT := lastTpt
		if lastTpt == 0 {
			nextT = minT
		} else {
			nextT = int(pwr2SeriesNext(p.config.tppo, uint64(lastTpt)))
		}
		delta := nextT - lastTpt
		for i := 0; i < delta; i++ {
			vIn = p.runTrial(vIn)
		}
		lastTpt = nextT
		sb := &strings.Builder{}
		if nextT < maxT {
			if p.config.interData {
				sb.Reset()
				p.setHeader(sb)
				p.process(lastTpt, sb)
				fmt.Println(sb.String())
			}
		} else {
			sb.Reset()
			p.setHeader(sb)
			p.process(lastTpt, sb)
			fmt.Println(sb.String())
		}

		fmt.Printf("Config:             : %+v\n", p.config)
		fmt.Printf("Cum Trials          : %d\n", lastTpt)
		fmt.Printf("Cum Updates         : %d\n", vIn)
		currentTime_mS := time.Now().UnixMilli()
		cumTime_mS := currentTime_mS - p.startTime
		fmt.Printf("Cum Time            : %s\n", time.Duration(cumTime_mS*1000*1000))
		timePerTrial_mS := float64(cumTime_mS) / float64(lastTpt)
		avgUpdateTime_ns := timePerTrial_mS * 1e6 / float64(maxU)
		fmt.Printf("Time Per Trial, mSec: %f\n", timePerTrial_mS)
		fmt.Printf("Avg Update Time, nSec: %f\n", avgUpdateTime_ns)
		fmt.Printf("Date Time           : %s\n", time.Now().Format(time.RFC3339))
		timeToComplete_mS := int64(timePerTrial_mS * float64(maxT-lastTpt))
		fmt.Printf("Est Time to Complete: %s\n", time.Duration(timeToComplete_mS*1000*1000))
		fmt.Printf("Est Time at Completion: %s\n", time.Now().Add(time.Duration(timeToComplete_mS*1000*1000)).Format(time.RFC3339))
		fmt.Println("")
	}
}

func (p *ThetaIntersectionAccuracyProfile) runTrial(key uint64) uint64 {
	p.smallSketch.Reset()
	p.largeSketch.Reset()

	lastUniques := uint64(0)
	for i, stat := range p.stats {
		delta := stat.trueValue - lastUniques
		for u := uint64(0); u < delta; u++ {
			key++
			if i == 0 {
				p.smallSketch.UpdateUint64(key)
			}
			p.largeSketch.UpdateUint64(key)
		}
		lastUniques += delta

		p.intersection.Update(p.smallSketch)
		p.intersection.Update(p.largeSketch)

		result, _ := p.intersection.OrderedResult()

		est := result.Estimate()
		lb3, _ := result.LowerBound(3)
		lb2, _ := result.LowerBound(2)
		lb1, _ := result.LowerBound(1)
		ub1, _ := result.UpperBound(1)
		ub2, _ := result.UpperBound(2)
		ub3, _ := result.UpperBound(3)
		stat.update(est, lb3, lb2, lb1, ub1, ub2, ub3)
	}

	return key
}

func (p *ThetaIntersectionAccuracyProfile) setHeader(sb *strings.Builder) string {
	sb.WriteString("TrueU")
	sb.WriteString("\t")
	sb.WriteString("MeanEst")
	sb.WriteString("\t")
	sb.WriteString("MeanRelErr")
	sb.WriteString("\t")
	sb.WriteString("RMS_RE")
	sb.WriteString("\t")
	sb.WriteString("Trials")
	sb.WriteString("\t")
	sb.WriteString("Min")
	sb.WriteString("\t")
	sb.WriteString("Q(.00135)")
	sb.WriteString("\t")
	sb.WriteString("Q(.02275)")
	sb.WriteString("\t")
	sb.WriteString("Q(.15866)")
	sb.WriteString("\t")
	sb.WriteString("Q(.5)")
	sb.WriteString("\t")
	sb.WriteString("Q(.84134)")
	sb.WriteString("\t")
	sb.WriteString("Q(.97725)")
	sb.WriteString("\t")
	sb.WriteString("Q(.99865)")
	sb.WriteString("\t")
	sb.WriteString("Max")
	sb.WriteString("\t")
	sb.WriteString("avgLB3")
	sb.WriteString("\t")
	sb.WriteString("avgLB2")
	sb.WriteString("\t")
	sb.WriteString("avgLB1")
	sb.WriteString("\t")
	sb.WriteString("avgUB1")
	sb.WriteString("\t")
	sb.WriteString("avgUB2")
	sb.WriteString("\t")
	sb.WriteString("avgUB3")
	sb.WriteString("\t")
	sb.WriteString("Max")
	return sb.String()
}

func (p *ThetaIntersectionAccuracyProfile) process(cumTrials int, sb *strings.Builder) {
	points := len(p.stats)
	for pt := 0; pt < points; pt++ {
		q := p.stats[pt]

		trueUniques := q.trueValue
		meanEst := q.sumEst / float64(cumTrials)
		meanRelErr := q.sumRelErr / float64(cumTrials)
		meanSqErr := q.sumSqRelErr / float64(cumTrials)
		normMeanSqErr := meanSqErr / (float64(trueUniques) * float64(trueUniques))
		rmsRelErr := math.Sqrt(normMeanSqErr)

		relLb3 := q.sumLB3/float64(cumTrials)/float64(trueUniques) - 1.0
		relLb2 := q.sumLB2/float64(cumTrials)/float64(trueUniques) - 1.0
		relLb1 := q.sumLB1/float64(cumTrials)/float64(trueUniques) - 1.0

		relUb1 := q.sumUB1/float64(cumTrials)/float64(trueUniques) - 1.0
		relUb2 := q.sumUB2/float64(cumTrials)/float64(trueUniques) - 1.0
		relUb3 := q.sumUB3/float64(cumTrials)/float64(trueUniques) - 1.0

		sb.WriteString(fmt.Sprintf("%d", trueUniques))
		sb.WriteString("\t")

		sb.WriteString(fmt.Sprintf("%e", meanEst))
		sb.WriteString("\t")

		sb.WriteString(fmt.Sprintf("%e", meanRelErr))
		sb.WriteString("\t")

		sb.WriteString(fmt.Sprintf("%e", rmsRelErr))
		sb.WriteString("\t")

		sb.WriteString(fmt.Sprintf("%d", cumTrials))
		sb.WriteString("\t")

		// Quantiles
		quants, _ := q.qsk.GetQuantiles(GAUSSIANS_3SD, true)
		for i := 0; i < len(quants); i++ {
			sb.WriteString(fmt.Sprintf("%e", quants[i]/float64(trueUniques)-1.0))
			sb.WriteString("\t")
		}

		// Bound averages
		sb.WriteString(fmt.Sprintf("%e", relLb3))
		sb.WriteString("\t")
		sb.WriteString(fmt.Sprintf("%e", relLb2))
		sb.WriteString("\t")
		sb.WriteString(fmt.Sprintf("%e", relLb1))
		sb.WriteString("\t")

		sb.WriteString(fmt.Sprintf("%e", relUb1))
		sb.WriteString("\t")
		sb.WriteString(fmt.Sprintf("%e", relUb2))
		sb.WriteString("\t")
		sb.WriteString(fmt.Sprintf("%e", relUb3))
		sb.WriteString("\n")
	}
}
