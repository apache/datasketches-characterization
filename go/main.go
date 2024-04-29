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
	"os"
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
			},
			hll.TgtHllTypeHll8,
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
