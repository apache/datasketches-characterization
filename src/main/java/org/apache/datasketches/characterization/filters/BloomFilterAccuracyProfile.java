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

package org.apache.datasketches.characterization.filters;

import org.apache.datasketches.filters.bloomfilter.BloomFilter;
import org.apache.datasketches.filters.bloomfilter.BloomFilterBuilder;

public class BloomFilterAccuracyProfile extends BaseFilterAccuracyProfile {

    protected BloomFilter sketch;

    protected long filterLengthBits;
//    protected int minNumBitsPerEntry;
//    protected int maxNumBitsPerEntry ;

    @Override
    public void configure() {
        filterLengthBits = Integer.parseInt(prop.mustGet("filterLengthBits"));
    }

    /*
     * The experiment has a fixed number of bits per entry and
     */
    @Override
    public double doTrial(final int  numHashes, final long numQueries) {

        //   and populate the sketch
        filterLengthBits = (long) ((numHashes * numItemsInserted) / LN2);
        sketch =  BloomFilterBuilder.createBySize(filterLengthBits, numHashes);

        // Build the test sets but clear them first so that they have the correct cardinality and no surplus is added.
        inputItems.clear();
        negativeItems.clear();
        long item;
        for (long i = 0; i < numItemsInserted; i++) {
            item = ++vIn;
            inputItems.add(item) ;
            sketch.update(item);
        }

        for (long i = numItemsInserted; i < numItemsInserted + numQueries; i++ ) {
            item = ++vIn;
            negativeItems.add(item) ;
        }

        // Check the number of false positives
        long numFalsePositive = 0 ;
        for (int i = 0; i < negativeItems.size(); i++) {
            if (sketch.query(negativeItems.get(i))) { ++numFalsePositive; }
        }
        return (double)numFalsePositive / numQueries;
    }

    @Override
    public int getBitsperEntry(final int numHashes) {
        return (int) (numHashes / LN2);
    }

    @Override
    public long getFilterLengthBits() {
        return sketch.getCapacity();
    }

}
