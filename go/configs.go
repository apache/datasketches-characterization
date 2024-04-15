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

type distinctCountJobConfigType struct {
	lgK int // lgK of distinct count sketch

	lgMinU int // The starting # of uniques that is printed at the end.
	lgMaxU int // How high the # uniques go
	UPPO   int // The horizontal x-resolution of trials points

	lgMinT int // prints intermediate results starting w/ this lgMinT
	lgMaxT int // The max trials
	TPPO   int // how often intermediate results are printed

	lgQK      int  // size of quantiles sketch
	interData bool // intermediate data

	numTrials             int
	numSketches           int
	distinctKeysPerSketch int
}

var (
	distinctCountJobConfig = distinctCountJobConfigType{
		lgMinU: 0,
		lgMaxU: 20,
		UPPO:   16,

		lgMinT: 8,
		lgMaxT: 20,
		TPPO:   1,

		lgQK:      12,
		interData: true,
	}
	distinctCountMergeJobConfig = distinctCountJobConfigType{
		lgK:                   12,
		numTrials:             100,
		numSketches:           8192,
		distinctKeysPerSketch: 32768,
	}
)
