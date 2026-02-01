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
	"slices"
	"sort"

	"github.com/apache/datasketches-go/tdigest"
)

type TDigestDoubleAccuracyProfile struct {
	config tdigestJobConfig
}

func MustNewTDigestDoubleAccuracyProfile(cfg tdigestJobConfig) *TDigestDoubleAccuracyProfile {
	return &TDigestDoubleAccuracyProfile{
		config: cfg,
	}
}

func (p *TDigestDoubleAccuracyProfile) run() {
	fmt.Print("N")
	for _, rank := range p.config.ranks {
		fmt.Printf("\terr at %.2f", rank)
	}
	fmt.Println()

	numSteps := countPoints(p.config.lgMin, p.config.lgMax, p.config.ppo)

	rankErrors := make([][]float64, len(p.config.ranks))
	for i := range rankErrors {
		rankErrors[i] = make([]float64, p.config.numTrials)
	}

	errorPctIndex := p.config.numTrials * p.config.errorPCT / 100

	streamLength := uint64(1)
	for step := 0; step < numSteps; step++ {
		for t := 0; t < p.config.numTrials; t++ {
			p.runTrial(streamLength, rankErrors, t)
		}

		fmt.Print(streamLength)
		for i := range p.config.ranks {
			sort.Float64s(rankErrors[i])
			rankError := rankErrors[i][errorPctIndex]
			fmt.Printf("\t%.6f", rankError*100)
		}
		fmt.Println()

		streamLength = pwr2SeriesNext(p.config.ppo, streamLength)
	}
}

func (p *TDigestDoubleAccuracyProfile) runTrial(streamLength uint64, rankErrors [][]float64, trialIndex int) {
	values := make([]float64, streamLength)
	for i := uint64(0); i < streamLength; i++ {
		values[i] = rand.ExpFloat64() / 1.5
	}

	sketch, _ := tdigest.NewDouble(uint16(p.config.k))
	for _, v := range values {
		sketch.Update(v)
	}

	slices.Sort(values)

	for j, rank := range p.config.ranks {
		quantile := getQuantile(values, rank)
		trueRank := getRankMidpoint(values, quantile)
		sketchRank, _ := sketch.Rank(quantile)
		rankErrors[j][trialIndex] = math.Abs(sketchRank - trueRank)
	}
}
