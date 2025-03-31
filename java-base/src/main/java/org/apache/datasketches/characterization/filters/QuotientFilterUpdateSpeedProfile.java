/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//package org.apache.datasketches.characterization.filters;
//
//import org.apache.datasketches.memory.WritableHandle;
//import org.apache.datasketches.memory.WritableMemory;
//
//
//import org.apache.datasketches.filters.quotientfilter.QuotientFilter;
//
//public class QuotientFilterUpdateSpeedProfile extends BaseFilterUpdateSpeedProfile{
//    protected QuotientFilter sketch;
//    protected int lgNumSlots ;
//    protected int numBitsPerSlot;
//    private WritableHandle handle;
//    private WritableMemory wmem;
//
//    @Override
//    public void configure() {
//        lgNumSlots = Integer.parseInt(prop.mustGet("lgNumSlots"));
//        numBitsPerSlot = Integer.parseInt(prop.mustGet("numBitsPerSlot"));
//    }
//
//    @Override
//    public void cleanup() {
//        try {
//            if (handle != null) { handle.close(); }
//        } catch (final Exception e) {}
//    }
//
//    @Override
//    public double doTrial(final int uPerTrial) {
//        //sketch.reset(); //is not implemented
//        sketch = new QuotientFilter(lgNumSlots, numBitsPerSlot);
//        final long startUpdateTime_nS = System.nanoTime();
//        for (int u = uPerTrial; u-- > 0;) {
//            sketch.insert(++vIn);
//        }
//        final long updateTime_nS = System.nanoTime() - startUpdateTime_nS;
//        return (double) updateTime_nS / uPerTrial;
//    }
//}

