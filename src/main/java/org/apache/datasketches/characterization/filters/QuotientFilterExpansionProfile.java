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
//import org.apache.datasketches.Job;
//import org.apache.datasketches.JobProfile;
//import org.apache.datasketches.filters.quotientfilter.QuotientFilter;
//
//import java.util.ArrayList;
//
//import static org.apache.datasketches.common.Util.pwr2SeriesNext;
//
//public class QuotientFilterExpansionProfile implements JobProfile{
//
//    private Job job;
//    long vIn = 0L;
//    int startLgU;
//    int endLgU;
//    int uPPO;
//    int lgK;
//    int startLenFprint;
//    public ArrayList<Long> negativeItems = new ArrayList<>();
//    int lgTrials;
//    int numTrials;
//
//    @Override
//    public void start(final Job job) {
//        this.job = job;
//        runExpansionTrials();
//    }
//
//    @Override
//    public void shutdown() { }
//
//    @Override
//    public void cleanup() { }
//
//    private void runExpansionTrials(){
//        //final ArrayList<Long> negativeItems = new ArrayList<>();
//        startLgU = Integer.parseInt(job.getProperties().mustGet("startLgU"));
//        endLgU = Integer.parseInt(job.getProperties().mustGet("endLgU"));
//        uPPO = Integer.parseInt(job.getProperties().mustGet("Universe_uPPO"));
//        lgK = Integer.parseInt(job.getProperties().mustGet("startLgNumSlots"));
//        startLenFprint = Integer.parseInt(job.getProperties().mustGet("startLenFprint"));
//        final int lgNumQueries = Integer.parseInt(job.getProperties().mustGet("lgNumQueries"));
//        lgTrials = Integer.parseInt(job.getProperties().mustGet("lgTrials"));
//        final long numQueries = 1L << lgNumQueries;
//        final int numTrials = 1 << lgTrials;
//
//        job.println(getHeader());
//        for (int t = 0; t < numTrials; t++)  doTrial(numQueries, startLgU, endLgU);
//    }
//
//    /*
//    The number of collisions in N keys inserted to a length M hash table is approximately N^2/2M.
//    This can be seen through the recursive relationship
//     */
//    private void doTrial(long numQueries, long startLgU, long endLgU){
//        QuotientFilter qf = new QuotientFilter(lgK, startLenFprint);
//        final StringBuilder dataStr = new StringBuilder();
//
//        // Populate the negative items.  Do this first to easily keep it separate from input item set.
//        negativeItems.clear();
//        for (long i = 0; i < numQueries; i++ ) {negativeItems.add(++vIn) ;}
//
//        // vIn is now the starting point; add a specified number of items from this location to the end of the range.
//        long startPoint = 0L;
//        long inputCardinality = (1L << startLgU) ;
//        long numInsertions;
//        long maxCardinality = (1L << endLgU) ; // end point
//
//        while(inputCardinality < maxCardinality) {
//            numInsertions = inputCardinality - startPoint;
//            for (long i = 0; i < numInsertions; i++) { qf.insert(++vIn);}
//            startPoint = inputCardinality;
//
//            // test the false positive rate
//            long numFalsePositive = 0;
//            for (long negItem : negativeItems) {
//                if (qf.search(negItem)) ++numFalsePositive;
//            }
//            //double fpr = (double) numFalsePositive / numQueries;
//            //System.out.println("Expansions " + qf.getNumExpansions() + " " + qf.getNumSlots())  ;
//            process(inputCardinality, qf.getNumEntries(), qf.getNumSlots(), qf.getFingerprintLength(), 
//                numFalsePositive, dataStr);
//            job.println(dataStr.toString());
//            inputCardinality = pwr2SeriesNext(uPPO, inputCardinality);
//        }
//    }
//
//    private static void process(final long numInput, final long numEntries, final long numSlots, final int fPrintLen,
//                                final long falsePositiveRate, final StringBuilder sb){
//                                //final double falsePositiveRate, final StringBuilder sb){
//        // OUTPUT
//        sb.setLength(0);
//        sb.append(numSlots).append(TAB);
//        sb.append(numInput).append(TAB);
//        sb.append(fPrintLen).append(TAB);
//        sb.append(numEntries).append(TAB);
//        sb.append(falsePositiveRate);
//    }
//
//    private String getHeader() {
//        final StringBuilder sb = new StringBuilder();
//        sb.append("NumSlots").append(TAB);
//        sb.append("NumInput").append(TAB);
//        sb.append("FPrintLen").append(TAB);
//        sb.append("NumEntries").append(TAB);
//        sb.append("NumFalsePositives");
//        //sb.append("FalsePositiveRate");
//        return sb.toString();
//    }
//}
