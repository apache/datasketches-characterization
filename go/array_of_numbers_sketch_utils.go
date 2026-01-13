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

import "github.com/apache/datasketches-go/theta"

type arrayOfNumbersSketchJobConfig struct {
	lgMinU int // The starting # of uniques that is printed at the end.
	lgMaxU int // How high the # uniques go
	uppo   int // The horizontal x-resolution of trials points

	lgMinT int // prints intermediate results starting w/ this lgMinT
	lgMaxT int // The max trials
	tppo   int // how often intermediate results are printed

	lgMinBpU int // start the downward slope of trials at this U
	lgMaxBpU int // stop the downward slope of trials at this U

	lgK          uint8
	numValues    uint8
	p            float32
	resizeFactor theta.ResizeFactor

	numSketches int
}
