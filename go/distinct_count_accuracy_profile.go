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
)

type DistinctCountAccuracyProfile struct {
	config    distinctCountJobConfigType
	runner    DistinctCountAccuracyProfileRunner
	stats     []baseAccuracyStats
	startTime int64
}

func NewDistinctCountAccuracyProfile(config distinctCountJobConfigType, runner DistinctCountAccuracyProfileRunner) *DistinctCountAccuracyProfile {
	return &DistinctCountAccuracyProfile{
		config:    config,
		runner:    runner,
		stats:     buildLog2AccuracyStatsArray(config.lgMinU, config.lgMaxU, config.UPPO, config.lgQK),
		startTime: time.Now().UnixMilli(),
	}
}

func (d *DistinctCountAccuracyProfile) run() {
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

func (d *DistinctCountAccuracyProfile) process(cumTrials int, sb *strings.Builder) {
	points := len(d.stats)
	for pt := 0; pt < points; pt++ {
		q := d.stats[pt].(*accuracyStats)

		trueUniques := q.trueValue

		meanEst := q.sumEst / float64(cumTrials)
		meanRelErr := q.sumRelErr / float64(cumTrials)
		meanSqErr := q.sumSqRelErr / float64(cumTrials)
		normMeanSqErr := meanSqErr / (float64(trueUniques) * float64(trueUniques))
		rmsRelErr := math.Sqrt(normMeanSqErr)
		q.rmse = rmsRelErr

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

		quants, _ := q.qsk.GetQuantiles(GAUSSIANS_4SD, true)
		for i := 0; i < len(quants); i++ {
			sb.WriteString(fmt.Sprintf("%e", float64(quants[i])/(float64(trueUniques))-1.0))
			sb.WriteString("\t")
		}

		sb.WriteString(fmt.Sprintf("%d", 0))
		sb.WriteString("\t")
		sb.WriteString(fmt.Sprintf("%d", 0))

		sb.WriteString("\n")
	}
}

func (d *DistinctCountAccuracyProfile) setHeader(sb *strings.Builder) string {
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
	sb.WriteString("Q(.0000317)")
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
	sb.WriteString("Q(.9999683)")
	sb.WriteString("\t")
	sb.WriteString("Max")
	sb.WriteString("\t")
	sb.WriteString("Bytes")
	sb.WriteString("\t")
	sb.WriteString("ReMerit")
	sb.WriteString("\n")
	return sb.String()
}

func buildLog2AccuracyStatsArray(lgMin, lgMax, ppo, lgQK int) []baseAccuracyStats {
	qLen := countPoints(lgMin, lgMax, ppo)
	qArr := make([]baseAccuracyStats, qLen)
	p := uint64(1) << lgMin
	for i := 0; i < qLen; i++ {
		qArr[i] = newAccuracyStats(1<<lgQK, p)
		p = pwr2SeriesNext(ppo, p)
	}
	return qArr
}
