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
	"os"

	"github.com/apache/datasketches-go/hll"
	"github.com/apache/datasketches-go/theta"
)

var (
	jobs = map[string]JobProfile{
		"distinct_count_accuracy_profile": NewDistinctCountAccuracyProfile(
			distinctCountJobConfigType{
				lgK: 11,

				lgMinU: 0,
				lgMaxU: 20,
				uppo:   16,

				lgMinT: 8,
				lgMaxT: 20,
				tppo:   1,

				lgQK:      12,
				interData: true,
			},
			hll.TgtHllTypeHll8,
		),
		"distinct_count_merge_accuracy_profile": NewDistinctCountMergeAccuracyProfile(
			distinctCountJobConfigType{
				lgK:                   12,
				numTrials:             100,
				numSketches:           8192,
				distinctKeysPerSketch: 32768,
			},
			hll.TgtHllTypeHll8,
		),
		"distinct_count_merge_speed_profile": NewDistinctCountMergeSpeedProfile(
			distinctCountJobConfigType{
				minLgK:   10,
				maxLgK:   21,
				lgMinT:   11,
				lgMaxT:   11,
				lgDeltaU: 2,
				serDe:    true,
			},
			hll.TgtHllTypeHll8,
		),
		"distinct_count_serde_profile": NewDistinctCountSerDeProfile(
			distinctCountJobConfigType{
				lgMinU: 0,
				lgMaxU: 20,
				uppo:   2,

				lgMaxT: 16,
				lgMinT: 7,

				lgMinBpU: 4,
				lgMaxBpU: 20,

				lgK:     12,
				compact: false,
			},
			hll.TgtHllTypeHll8,
		),
		"frequency_long_speed_profile": NewFrequencyLongSpeedProfile(
			frequencyJobConfigType{
				k:            1024,
				zipfRange:    8192,
				zipfExponent: 1.1,

				lgMin: 0,
				lgMax: 23,
				PPO:   16,

				lgMaxTrials: 16,
				lgMinTrials: 8,
			},
		),
		"theta_accuracy_profile": MustNewThetaAccuracyProfile(
			thetaJobConfig{
				lgMinU: 0,
				lgMaxU: 20,
				uppo:   16,

				lgMinT: 8,
				lgMaxT: 18,
				tppo:   1,

				interData: true,
				lgQK:      12,

				lgK:          12,
				p:            1,
				resizeFactor: theta.ResizeX1,
			},
		),
		"theta_update_speed_profile": MustNewThetaUpdateSpeedProfile(
			thetaJobConfig{
				lgMinU: 0,
				lgMaxU: 23,
				uppo:   16,

				lgMinT: 4,
				lgMaxT: 23,

				lgMinBpU: 4,
				lgMaxBpU: 20,

				lgK:          12,
				p:            1.0,
				resizeFactor: theta.ResizeX1,
			},
		),
		"theta_union_accuracy_profile": MustNewThetaUnionAccuracyProfile(
			thetaJobConfig{
				lgMinU: 0,
				lgMaxU: 24,
				uppo:   16,

				lgMinT: 8,
				lgMaxT: 10,
				tppo:   1,

				lgQK:      10,
				interData: true,

				lgK:          19,
				p:            1,
				resizeFactor: theta.ResizeX1,
			},
		),
		"theta_union_update_speed_profile": MustNewThetaUnionUpdateSpeedProfile(
			thetaJobConfig{
				lgMinU: 0,
				lgMaxU: 24,
				uppo:   16,

				lgMinT: 6,
				lgMaxT: 14,

				lgMinBpU: 0,
				lgMaxBpU: 24,

				lgK:         12,
				numSketches: 32,
			},
		),
		"theta_intersection_accuracy_profile": MustNewIntersectionThetaAccuracyProfile(
			thetaJobConfig{
				isIntersectionProfile: true,

				lgMinU: 16,
				lgMaxU: 30,
				uppo:   1,

				lgMinT: 2,
				lgMaxT: 9,
				tppo:   4,

				lgQK:      12,
				interData: true,

				lgK:          12,
				p:            1,
				resizeFactor: theta.ResizeX1,
			},
		),
		"array_of_numbers_update_speed_profile": MustNewArrayOfNumberUpdateSpeedProfile(
			arrayOfNumbersSketchJobConfig{
				lgMinU: 0,
				lgMaxU: 23,
				uppo:   16,

				lgMaxT: 23,
				lgMinT: 4,

				lgMinBpU: 4,
				lgMaxBpU: 20,

				lgK:          12,
				numValues:    1,
				resizeFactor: theta.ResizeX8,
				p:            1.0,
			},
		),
		"array_of_numbers_union_update_speed_profile": MustNewArrayOfNumberUpdateSpeedProfile(
			arrayOfNumbersSketchJobConfig{
				lgMinU: 0,
				lgMaxU: 24,
				uppo:   16,

				lgMaxT: 14,
				lgMinT: 6,

				lgMinBpU: 0,
				lgMaxBpU: 24,

				lgK:          12,
				numValues:    1,
				resizeFactor: theta.ResizeX8,
				p:            1.0,

				numSketches: 32,
			},
		),
	}
)

func usage() {
	fmt.Println("Usage: go run main.go <job>")
	fmt.Println("Available jobs:")
	for job := range jobs {
		fmt.Println(fmt.Sprintf("\t%s", job))
	}
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 || os.Args[1] == "-h" || os.Args[1] == "--help" {
		usage()
	}

	job, ok := jobs[os.Args[1]]
	if !ok {
		usage()
	}

	job.run()
}
