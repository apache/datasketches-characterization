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
	"strings"
	"time"
)

type DistinctCountBoundsAccuracyProfile struct {
	config    distinctCountJobConfigType
	runner    DistinctCountAccuracyProfileRunner
	stats     []baseAccuracyStats
	startTime int64
}

func NewDistinctCountBoundsAccuracyProfile(config distinctCountJobConfigType, runner DistinctCountAccuracyProfileRunner) *DistinctCountBoundsAccuracyProfile {
	return &DistinctCountBoundsAccuracyProfile{
		config:    config,
		runner:    runner,
		stats:     buildLog2BoundsAccuracyStatsArray(config.lgMinU, config.lgMaxU, config.UPPO, config.lgQK),
		startTime: time.Now().UnixMilli(),
	}
}

func (d *DistinctCountBoundsAccuracyProfile) run() {
	minT := 1 << d.config.lgMinT
	maxT := 1 << d.config.lgMaxT
	maxU := 1 << d.config.lgMaxU

	vIn := uint64(0)

	// This will generate a table of data for each intermediate Trials point
	lastTpt := 0
	for lastTpt < maxT {
		nextT := lastTpt
		if lastTpt == 0 {
			nextT = minT
		} else {
			nextT = int(pwr2SeriesNext(d.config.TPPO, uint64(lastTpt)))
		}
		delta := nextT - lastTpt
		for i := 0; i < delta; i++ {
			vIn = d.runner.runTrial(d.stats, vIn)
		}
		lastTpt = nextT
		sb := &strings.Builder{}
		if nextT < maxT {
			if d.config.interData {
				sb.Reset()
				d.setHeader(sb)
				d.process(lastTpt, sb)
				fmt.Println(sb.String())
			}
		} else {
			sb.Reset()
			d.setHeader(sb)
			d.process(lastTpt, sb)
			fmt.Println(sb.String())
		}

		fmt.Printf("Config:             : %+v\n", d.config)
		fmt.Printf("Cum Trials          : %d\n", lastTpt)
		fmt.Printf("Cum Updates         : %d\n", vIn)
		currentTime_mS := time.Now().UnixMilli()
		cumTime_mS := currentTime_mS - d.startTime
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

func (d *DistinctCountBoundsAccuracyProfile) process(cumTrials int, sb *strings.Builder) {
	points := len(d.stats)
	for pt := 0; pt < points; pt++ {
		q := d.stats[pt].(*boundsAccuracyStats)

		trueUniques := q.trueValue
		relLb3 := q.sumLB3/float64(cumTrials)/float64(trueUniques) - 1.0
		relLb2 := q.sumLB2/float64(cumTrials)/float64(trueUniques) - 1.0
		relLb1 := q.sumLB1/float64(cumTrials)/float64(trueUniques) - 1.0

		relLUb1 := q.sumUB1/float64(cumTrials)/float64(trueUniques) - 1.0
		relLUb2 := q.sumUB2/float64(cumTrials)/float64(trueUniques) - 1.0
		relLUb3 := q.sumUB3/float64(cumTrials)/float64(trueUniques) - 1.0

		// OUTPUT
		sb.WriteString(fmt.Sprintf("%d", trueUniques))
		sb.WriteString("\t")
		// TRIALS
		sb.WriteString(fmt.Sprintf("%d", cumTrials))
		sb.WriteString("\t")

		// Quantiles
		quants, _ := q.qsk.GetQuantiles(GAUSSIANS_3SD, true)
		for i := 0; i < len(quants); i++ {
			sb.WriteString(fmt.Sprintf("%f", quants[i]/float64(trueUniques)-1.0))
			sb.WriteString("\t")
		}

		// Bound averages
		sb.WriteString(fmt.Sprintf("%f", relLb3))
		sb.WriteString("\t")
		sb.WriteString(fmt.Sprintf("%f", relLb2))
		sb.WriteString("\t")
		sb.WriteString(fmt.Sprintf("%f", relLb1))
		sb.WriteString("\t")

		sb.WriteString(fmt.Sprintf("%f", relLUb1))
		sb.WriteString("\t")
		sb.WriteString(fmt.Sprintf("%f", relLUb2))
		sb.WriteString("\t")
		sb.WriteString(fmt.Sprintf("%f", relLUb3))
		sb.WriteString("\n")

	}
}

func (d *DistinctCountBoundsAccuracyProfile) setHeader(sb *strings.Builder) string {
	sb.WriteString("InU")
	sb.WriteString("\t")
	sb.WriteString("Trials")
	sb.WriteString("\t")
	sb.WriteString("Min")
	sb.WriteString("\t")
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
	sb.WriteString("\n")
	return sb.String()
}

func buildLog2BoundsAccuracyStatsArray(lgMin, lgMax, ppo, lgQK int) []baseAccuracyStats {
	qLen := countPoints(lgMin, lgMax, ppo)
	qArr := make([]baseAccuracyStats, qLen)
	p := uint64(1) << lgMin
	for i := 0; i < qLen; i++ {
		qArr[i] = newBoundsAccuracyStats(1<<lgQK, p)
		p = pwr2SeriesNext(ppo, p)
	}
	return qArr
}
