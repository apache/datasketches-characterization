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
	"time"
)

type DistinctCountAccuracyProfileRunner interface {
	runTrial(stats []accuracyStats, key uint64) uint64
}

type accuracyStats struct {
	trueValue   int
	sumEst      float64
	sumRelErr   float64
	sumSqRelErr float64
	count       int
	//kll_sketch<double> rel_err_distribution;
}

type DistinctCountAccuracyProfile struct {
	runner DistinctCountAccuracyProfileRunner
	stats  []accuracyStats
}

func NewDistinctCountAccuracyProfile(runner DistinctCountAccuracyProfileRunner) *DistinctCountAccuracyProfile {
	return &DistinctCountAccuracyProfile{
		runner,
		make([]accuracyStats, 0),
	}
}

func (d *DistinctCountAccuracyProfile) run() {
	const (
		lgMinTrials       = 4
		lgMaxTrials       = 16
		trialsPpo         = 4
		printIntermediate = true

		minT      = uint64(1) << lgMinTrials
		maxTrials = 1 << lgMaxTrials

		lgMinCounts = 0
		lgMaxCounts = 32
		countsPpo   = 16

		quantilesK = 10000
	)

	var (
		numPoints = countPoints(lgMinCounts, lgMaxCounts, countsPpo)
		p         = uint64(1) << lgMinCounts
		key       = uint64(0)
	)

	for i := 0; i < numPoints; i++ {
		d.stats = append(d.stats, accuracyStats{
			trueValue: quantilesK,
			//rel_err_distribution: p,
		})
		p = pwr2LawNext(countsPpo, p)
	}

	startTime := time.Now().UnixMilli()

	// this will generate a table of data up to each intermediate number of trials
	lastTrials := uint64(0)
	for lastTrials < maxTrials {
		nextTrials := minT
		if lastTrials != 0 {
			nextTrials = pwr2LawNext(trialsPpo, lastTrials)
		}
		delta := nextTrials - lastTrials
		for i := uint64(0); i < delta; i++ {
			key = d.runner.runTrial(d.stats, key)
		}
		lastTrials = nextTrials
		if printIntermediate || nextTrials == maxTrials {
			d.printStats()
		}

		fmt.Println("Cum Trials             : ", lastTrials)
		fmt.Println("Cum Updates            : ", key)
		cumTimeMs := time.Now().UnixMilli() - startTime
		fmt.Println("Cum Time, ms           : ", cumTimeMs)
		timePerTrialMs := float64(cumTimeMs) / float64(lastTrials)
		fmt.Println("Avg Time Per Trial, ms : ", timePerTrialMs)
		currentTime := time.Now()
		fmt.Println("Current time           : ", currentTime)
		timeToCompleteMs := float64(cumTimeMs) / float64(lastTrials) * float64(maxTrials-lastTrials)
		estCompletionTime := currentTime.Add(time.Duration(timeToCompleteMs) * time.Millisecond)
		fmt.Println("Est Time of Completion : ", estCompletionTime)
		fmt.Println()
	}
}

func (d *DistinctCountAccuracyProfile) printStats() {
	for _, stat := range d.stats {
		fmt.Println(stat.trueValue)
		fmt.Println(stat.count)
		fmt.Println(stat.sumEst)
		fmt.Println(stat.sumRelErr)
		fmt.Println(stat.sumSqRelErr)
		// quantiles
		//const auto quants = stat.get_quantiles(FRACTIONS, FRACT_LEN);
		//for (size_t i = 0; i < FRACT_LEN; i++) {
		//	const double quantile = quants[i];
		//	std::cout << quantile;
		//	if (i != FRACT_LEN - 1) std::cout << "\t";
		//}
		fmt.Println()
	}
}
