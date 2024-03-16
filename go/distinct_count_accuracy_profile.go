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
	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/kll"
	"math"
	"strings"
	"time"
)

const (
	M4SD = 0.0000316712418331 //minus 4 StdDev
	M3SD = 0.0013498980316301 //minus 3 StdDev
	M2SD = 0.0227501319481792 //minus 2 StdDev
	M1SD = 0.1586552539314570 //minus 1 StdDev
	MED  = 0.5                //median
	P1SD = 0.8413447460685430 //plus  1 StdDev
	P2SD = 0.9772498680518210 //plus  2 StdDev
	P3SD = 0.9986501019683700 //plus  3 StdDev
	P4SD = 0.9999683287581670 //plus  4 StdDev
)

var (
	GAUSSIANS_4SD = []float64{0.0, M4SD, M3SD, M2SD, M1SD, MED, P1SD, P2SD, P3SD, P4SD, 1.0}
)

type DistinctCountAccuracyProfileRunner interface {
	runTrial(stats []*accuracyStats, key uint64) uint64
}

type accuracyStats struct {
	trueValue   uint64
	sumEst      float64
	sumRelErr   float64
	sumSqRelErr float64
	count       int
	// Make that a sketch of float64
	rel_err_distribution *kll.ItemsSketch[int64]
}

func (a *accuracyStats) update(est float64) {
	a.sumEst += est
	relativeError := est/float64(a.trueValue) - 1.0
	a.sumRelErr += relativeError
	a.sumSqRelErr += relativeError * relativeError
	a.rel_err_distribution.Update(int64(relativeError))
	a.count++
}

type DistinctCountAccuracyProfile struct {
	config    distinctCountJobConfigType
	runner    DistinctCountAccuracyProfileRunner
	stats     []*accuracyStats
	startTime int64
}

func NewDistinctCountAccuracyProfile(config distinctCountJobConfigType) *DistinctCountAccuracyProfile {
	return &DistinctCountAccuracyProfile{
		config:    config,
		runner:    config.runner,
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
				fmt.Println(getHeader())
				process(d.stats, lastTpt, sb)
				fmt.Println(sb.String())
			}
		} else {
			fmt.Println(getHeader())
			process(d.stats, lastTpt, sb)
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

func process(qArr []*accuracyStats, cumTrials int, sb *strings.Builder) {
	points := len(qArr)
	sb.Reset()
	for pt := 0; pt < points; pt++ {
		q := qArr[pt]

		trueUniques := q.trueValue

		meanEst := q.sumEst / float64(cumTrials)
		meanRelErr := q.sumRelErr / float64(cumTrials)
		meanSqErr := q.sumSqRelErr / float64(cumTrials)
		normMeanSqErr := meanSqErr / (float64(trueUniques) * float64(trueUniques))
		rmsRelErr := math.Sqrt(normMeanSqErr)
		//q.rmsre = rmsRelErr

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

		quants, _ := q.rel_err_distribution.GetQuantiles(GAUSSIANS_4SD, true)
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

func getHeader() string {
	sb := &strings.Builder{}
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
	return sb.String()
}

func buildLog2AccuracyStatsArray(lgMin, lgMax, ppo, lgQK int) []*accuracyStats {
	qLen := countPoints(lgMin, lgMax, ppo)
	qArr := make([]*accuracyStats, qLen)
	p := uint64(1) << lgMin
	for i := 0; i < qLen; i++ {
		kllSketch, _ := kll.NewKllItemsSketch[int64](uint16(lgQK), 8, common.ArrayOfLongsSerDe{})
		qArr[i] = &accuracyStats{
			trueValue:            p,
			rel_err_distribution: kllSketch,
		}
		p = pwr2SeriesNext(ppo, p)
	}
	return qArr
}
