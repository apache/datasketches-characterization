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
	"testing"
)

func TestHllSketchAccuracyRunner(t *testing.T) {
	jobs["distinct_count_accuracy_profile"].run()
}

func TestHllSketchMergeAccuracyRunner(t *testing.T) {
	jobs["distinct_count_merge_accuracy_profile"].run()
}

func TestHllSketchMergeSpeedRunner(t *testing.T) {
	jobs["distinct_count_merge_speed_profile"].run()
}

func TestHllSketchSerdeRunner(t *testing.T) {
	jobs["distinct_count_serde_profile"].run()
}

func TestFrequencyLongSpeedRunner(t *testing.T) {
	jobs["frequency_long_speed_profile"].run()
}

func TestThetaSketchAccuracyRunner(t *testing.T) {
	jobs["theta_accuracy_profile"].run()
}

func TestThetaSketchUpdateSpeedRunner(t *testing.T) {
	jobs["theta_update_speed_profile"].run()
}

func TestThetaSketchUnionAccuracyRunner(t *testing.T) {
	jobs["theta_union_accuracy_profile"].run()
}

func TestThetaSketchUnionUpdateSpeedRunner(t *testing.T) {
	jobs["theta_union_update_speed_profile"].run()
}

func TestThetaSketchIntersectionAccuracyRunner(t *testing.T) {
	jobs["theta_intersection_accuracy_profile"].run()
}

func TestArrayOfNumbersUpdateSpeedRunner(t *testing.T) {
	jobs["array_of_numbers_update_speed_profile"].run()
}

func TestArrayOfNumbersUnionUpdateSpeedRunner(t *testing.T) {
	jobs["array_of_numbers_union_update_speed_profile"].run()
}
