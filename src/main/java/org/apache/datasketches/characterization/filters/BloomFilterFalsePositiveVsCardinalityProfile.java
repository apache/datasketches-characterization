package org.apache.datasketches.characterization.filters;
import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.filters.bloomfilter.BloomFilter;
import org.apache.datasketches.filters.bloomfilter.BloomFilterBuilder;

import java.util.ArrayList;

import static org.apache.datasketches.common.Util.pwr2SeriesNext;

public class BloomFilterFalsePositiveVsCardinalityProfile implements JobProfile {
    /*
    This class generates a profile for the Bloom filter that is analogous toe the QuotientFilterExpansionProfile.java
    However, the Bloom filter does not have expansion capability, hence the reason for the name change.
    */


    private Job job;
    long vIn = 0L;
    int startLgU;
    int endLgU;
    int uPPO;
    public ArrayList<Long> negativeItems = new ArrayList<>();

    BloomFilter bf;
    long lgExpectedCardinality;
    long numFilterBits;
    short numHashes;
    int negativeLgFppTarget;

    @Override
    public void start(final Job job) {
        this.job = job;
        runTrials();
    }

    private void runTrials(){
        lgExpectedCardinality = Integer.parseInt(job.getProperties().mustGet("lgExpectedCardinality"));
        startLgU = Integer.parseInt(job.getProperties().mustGet("startLgU"));
        endLgU = Integer.parseInt(job.getProperties().mustGet("endLgU"));
        uPPO = Integer.parseInt(job.getProperties().mustGet("Universe_uPPO"));
        negativeLgFppTarget = Integer.parseInt(job.getProperties().mustGet("negativeLgFppTarget"));

        // derived quantities
        final long cardinalityEstimate = 1L<<lgExpectedCardinality;
        final double targetFpp = Math.pow(2.0, -negativeLgFppTarget);
        numFilterBits = BloomFilterBuilder.suggestNumFilterBits(cardinalityEstimate, targetFpp) ;
        numHashes = BloomFilterBuilder.suggestNumHashes(cardinalityEstimate, numFilterBits);

        // Set up the trials
        final int lgNumQueries = Integer.parseInt(job.getProperties().mustGet("lgNumQueries"));
        final int lgTrials = Integer.parseInt(job.getProperties().mustGet("lgTrials"));
        final long numQueries = 1L << lgNumQueries;
        final int numTrials = 1 << lgTrials;

        job.println(getHeader());
        bf = BloomFilterBuilder.createByAccuracy(cardinalityEstimate, targetFpp);
        for (int t = 0; t < numTrials; t++)  doTrial(numQueries, startLgU, endLgU);
    }

    /*
    nb. Lines 69-78 (dataStr to numInsertions) duplicate lines 58-67 in QuotientFilterExpansionProfile.java
    Duplication could be avoided by refactoring, but the filter logic was sufficiently different that @cdickens opted
    to keep them separate.
     */
    private void doTrial(long numQueries, long startLgU, long endLgU){
        bf.reset(); // clear the filter
        final StringBuilder dataStr = new StringBuilder();

        // Populate the negative items.  Do this first to easily keep it separate from input item set.
        negativeItems.clear();
        for (long i = 0; i < numQueries; i++ ) {negativeItems.add(++vIn) ;}

        // vIn is now the starting point; add a specified number of items from this location to the end of the range.
        long startPoint = 0L;
        long inputCardinality = (1L << startLgU) ;
        long numInsertions;
        long maxCardinality = Math.min(1L << endLgU, numFilterBits) ; // end point

        while(inputCardinality < maxCardinality) {
            numInsertions = inputCardinality - startPoint;
            for (long i = 0; i < numInsertions; i++) { bf.update(++vIn); }
            startPoint = inputCardinality;

            // test the false positive rate
            long numFalsePositive = 0;
            for (long negItem : negativeItems) { if (bf.query(negItem)) ++numFalsePositive; }
            process(inputCardinality, numFilterBits, numHashes, numFalsePositive, bf.getBitsUsed(), dataStr);
            job.println(dataStr.toString());
            inputCardinality = pwr2SeriesNext(uPPO, inputCardinality);
        }
    }


    @Override
    public void shutdown() { }

    @Override
    public void cleanup() { }

    private static void process(final long numInput, final long numFilterBits, final short numHashes,
                                final long numFalsePositives, final long numBitsSet, final StringBuilder sb){
        // OUTPUT
        sb.setLength(0);
        sb.append(numInput).append(TAB);
        sb.append(numFilterBits).append(TAB);
        sb.append(numHashes).append(TAB);
        sb.append(numFalsePositives).append(TAB);
        sb.append(numBitsSet);
    }

    private String getHeader() {
        final StringBuilder sb = new StringBuilder();
        sb.append("NumInput").append(TAB);
        sb.append("NumFilterBits").append(TAB);
        sb.append("NumHashes").append(TAB);
        sb.append("NumFalsePositives").append(TAB);
        sb.append("NumBitsSet");
        return sb.toString();
    }


}
