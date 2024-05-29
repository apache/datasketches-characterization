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
	"github.com/apache/datasketches-go/hll"
	"math"
	"math/rand/v2"
	"time"
)

type DistinctCountMergeAccuracyProfile struct {
	config    distinctCountJobConfigType
	startTime int64
	tgtType   hll.TgtHllType
}

func NewDistinctCountMergeAccuracyProfile(config distinctCountJobConfigType, tgtType hll.TgtHllType) *DistinctCountMergeAccuracyProfile {
	return &DistinctCountMergeAccuracyProfile{
		config:    config,
		tgtType:   tgtType,
		startTime: time.Now().UnixMilli(),
	}
}

func (d *DistinctCountMergeAccuracyProfile) run() {
	key := rand.Int64()
	trueCount := d.config.numSketches * d.config.distinctKeysPerSketch

	var (
		sumEstimates                        float64
		sumOfSquaredDeviationsFromTrueCount float64
	)

	for t := 0; t < d.config.numTrials; t++ {
		union, _ := hll.NewUnion(d.config.lgK)

		for s := 0; s < d.config.numSketches; s++ {
			sk, _ := hll.NewHllSketch(d.config.lgK, d.tgtType)
			for k := 0; k < d.config.distinctKeysPerSketch; k++ {
				sk.UpdateInt64(key)
				key += 1
			}
			union.UpdateSketch(sk)
		}
		skRes, _ := union.GetResult(hll.TgtHllTypeDefault)
		estimatedCount, _ := skRes.GetEstimate()
		sumEstimates += estimatedCount
		sumOfSquaredDeviationsFromTrueCount += (estimatedCount - float64(trueCount)) * (estimatedCount - float64(trueCount))
	}

	meanEstimate := sumEstimates / float64(d.config.numTrials)
	meanRelativeError := meanEstimate/float64(trueCount) - 1
	relativeStandardError := math.Sqrt(sumOfSquaredDeviationsFromTrueCount/float64(d.config.numTrials)) / float64(trueCount)

	fmt.Println(fmt.Sprintf("True count: %d", trueCount))
	fmt.Println(fmt.Sprintf("Mean Estimate: %f", meanEstimate))
	fmt.Println(fmt.Sprintf("Mean Relative Error: %f", meanRelativeError))
	fmt.Println(fmt.Sprintf("Relative Standard Error: %f", relativeStandardError))
}
