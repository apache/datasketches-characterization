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

	"github.com/apache/datasketches-go/common"
	"github.com/apache/datasketches-go/hll"
	"github.com/apache/datasketches-go/kll"
)

type DistinctCountAccuracyProfile struct {
	config    distinctCountJobConfigType
	sketch    hll.HllSketch
	stats     []baseAccuracyStats
	startTime int64
}

type accuracyStats struct {
	qsk         *kll.ItemsSketch[float64]
	sumLB3      float64
	sumLB2      float64
	sumLB1      float64
	sumUB3      float64
	sumUB2      float64
	sumUB1      float64
	sumEst      float64
	sumRelErr   float64
	sumSqRelErr float64
	trueValue   uint64
}

func NewDistinctCountAccuracyProfile(config distinctCountJobConfigType, tgtType hll.TgtHllType) *DistinctCountAccuracyProfile {
	sketch, _ := hll.NewHllSketch(config.lgK, tgtType)
	return &DistinctCountAccuracyProfile{
		config:    config,
		sketch:    sketch,
		stats:     buildLog2AccuracyStatsArray(config.lgMinU, config.lgMaxU, config.uppo, config.lgQK),
		startTime: time.Now().UnixMilli(),
	}
}

func newAccuracyStats(k int, trueValue uint64) *accuracyStats {
	qsk, _ := kll.NewKllItemsSketch[float64](uint16(k), 8, common.ItemSketchDoubleComparator(false), common.ItemSketchDoubleSerDe{})
	return &accuracyStats{
		qsk:       qsk,
		trueValue: trueValue,
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
			nextT = int(pwr2SeriesNext(d.config.tppo, uint64(lastTpt)))
		}
		delta := nextT - lastTpt
		for i := 0; i < delta; i++ {
			vIn = d.runTrial(vIn)
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

func (d *DistinctCountAccuracyProfile) runTrial(key uint64) uint64 {
	d.sketch.Reset()

	lastUniques := uint64(0)
	for _, ostat := range d.stats {
		stat := ostat.(*accuracyStats)
		delta := stat.trueValue - lastUniques
		for u := uint64(0); u < delta; u++ {
			key++
			d.sketch.UpdateUInt64(key)
		}
		lastUniques += delta
		est, _ := d.sketch.GetEstimate()
		lb3, _ := d.sketch.GetLowerBound(3)
		lb2, _ := d.sketch.GetLowerBound(2)
		lb1, _ := d.sketch.GetLowerBound(1)

		ub1, _ := d.sketch.GetUpperBound(1)
		ub2, _ := d.sketch.GetUpperBound(2)
		ub3, _ := d.sketch.GetUpperBound(3)

		stat.update(est, lb3, lb2, lb1, ub1, ub2, ub3)
	}

	return key
}

func (a *accuracyStats) update(
	est float64,
	lb3 float64,
	lb2 float64,
	lb1 float64,
	ub1 float64,
	ub2 float64,
	ub3 float64,
) {
	a.qsk.Update(est)
	a.sumLB3 += lb3
	a.sumLB2 += lb2
	a.sumLB1 += lb1
	a.sumUB1 += ub1
	a.sumUB2 += ub2
	a.sumUB3 += ub3
	a.sumEst += est
	a.sumRelErr += est/float64(a.trueValue) - 1.0
	erro := est - float64(a.trueValue)
	a.sumSqRelErr += erro * erro
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
