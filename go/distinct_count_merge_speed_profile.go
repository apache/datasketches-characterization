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
	"strings"
	"time"
)

type DistinctCountMergeSpeedProfile struct {
	config    distinctCountJobConfigType
	union     hll.Union
	source    hll.HllSketch
	startTime int64
}

type mergeSpeedStats struct {
	mergeTime_nS float64
	totalTime_nS float64
}

func NewDistinctCountMergeSpeedProfile(config distinctCountJobConfigType, tgtType hll.TgtHllType) *DistinctCountMergeSpeedProfile {
	union, _ := hll.NewUnion(21)
	return &DistinctCountMergeSpeedProfile{
		config:    config,
		union:     union,
		startTime: time.Now().UnixMilli(),
	}
}

func (d *DistinctCountMergeSpeedProfile) run() {
	sb := &strings.Builder{}
	d.setHeader(sb)
	fmt.Println(sb.String())

	stats := &mergeSpeedStats{}
	vIn := int64(0)
	for lgK := d.config.minLgK; lgK <= d.config.maxLgK; lgK++ {
		var (
			lgT             = d.config.maxLgK - lgK + d.config.lgMinT
			trials          = 1 << lgT
			sumMergeTime_nS = 0.0
			sumTotalTime_nS = 0.0
		)
		sb.Reset()
		vIn = d.resetMerge(lgK, vIn)
		for t := 0; t < trials; t++ {
			d.runTrial(stats, lgK, d.config.lgDeltaU)
			sumMergeTime_nS += stats.mergeTime_nS
			sumTotalTime_nS += stats.totalTime_nS
		}
		stats.mergeTime_nS = sumMergeTime_nS / float64(trials)
		stats.totalTime_nS = sumTotalTime_nS / float64(trials)
		d.process(stats, lgK, lgT, sb)
		fmt.Println(sb.String())
	}
}

func (d *DistinctCountMergeSpeedProfile) setHeader(sb *strings.Builder) string {
	sb.WriteString("LgK")
	sb.WriteString("\t")
	sb.WriteString("LgT")
	sb.WriteString("\t")
	sb.WriteString("Merge_nS")
	sb.WriteString("\t")
	sb.WriteString("Total_nS")
	sb.WriteString("\t")
	sb.WriteString("PerSlot_nS")
	return sb.String()
}

func (d *DistinctCountMergeSpeedProfile) runTrial(stats *mergeSpeedStats, lgK int, lgDeltaU int) {
	start := uint64(0)
	mergeTime_nS := uint64(0)

	start = uint64(time.Now().UnixNano())
	d.union.UpdateSketch(d.source)
	mergeTime_nS = uint64(time.Now().UnixNano()) - start

	stats.mergeTime_nS = float64(mergeTime_nS)
	stats.totalTime_nS = float64(mergeTime_nS)
}
func (d *DistinctCountMergeSpeedProfile) process(stats *mergeSpeedStats, lgK int, lgT int, sb *strings.Builder) string {
	sb.WriteString(fmt.Sprintf("%d", lgK))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%d", lgT))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%e", stats.mergeTime_nS))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%e", stats.totalTime_nS))
	sb.WriteString("\t")
	sb.WriteString(fmt.Sprintf("%e", stats.totalTime_nS/float64(uint64(1<<lgK))))
	return sb.String()
}

func (d *DistinctCountMergeSpeedProfile) resetMerge(lgK int, vIn int64) int64 {
	d.union, _ = hll.NewUnion(lgK)
	d.source, _ = hll.NewHllSketch(lgK, hll.TgtHllTypeDefault)
	U := 2 << lgK
	for i := 0; i < U; i++ {
		vIn++
		d.union.UpdateInt64(vIn)
		d.source.UpdateInt64(vIn)
	}
	return vIn
}
