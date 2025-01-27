package org.apache.datasketches.characterization.filters;

import org.apache.datasketches.filters.bloomfilter.BloomFilter;
import org.apache.datasketches.filters.bloomfilter.BloomFilterBuilder;

import static org.apache.datasketches.common.Util.pwr2SeriesNext;

public class BloomFilterSpaceProfile extends BaseSpaceProfile {
    protected BloomFilter sketch;

    public void configure() {}

    @Override
    //public long doTrial(long inputCardinality, double targetFpp) {
    public trialResults doTrial(long inputCardinality, double targetFpp){
        trialResults result = new trialResults();
        final long numBits = BloomFilterBuilder.suggestNumFilterBits(inputCardinality, targetFpp);
        final short numHashes = BloomFilterBuilder.suggestNumHashes(inputCardinality, numBits);
        sketch = BloomFilterBuilder.createBySize(numBits, (int)numHashes-4, 348675132L);

        //sketch = BloomFilterBuilder.createByAccuracy(inputCardinality, targetFpp);
        long numQueries = pwr2SeriesNext(1, 1L<<(sketch.getNumHashes()+4));
        // Build the test sets but clear them first so that they have the correct cardinality and no surplus is added.
        inputItems.clear();
        negativeItems.clear();
        long item;
        for (long i = 0; i < inputCardinality; i++){
            item = ++vIn;
            inputItems.add(item) ;
            sketch.update(item);
        }

        for (long i = inputCardinality; i < inputCardinality+numQueries; i++ ) {
            item = ++vIn;
            negativeItems.add(item) ;
        }

        // Check the number of false positives
        long numFalsePositive = 0 ;
        for (int i = 0; i < negativeItems.size(); i++){
            if (sketch.query(negativeItems.get(i))) ++numFalsePositive ;
        }
        result.filterSizeBits = sketch.getCapacity();
        result.measuredFalsePositiveRate = (double)numFalsePositive / numQueries;
        result.numHashbits = sketch.getNumHashes();
        return result;
    }

}