package org.apache.datasketches.characterization.filters;

import org.apache.datasketches.filters.bloomfilter.BloomFilter;
import org.apache.datasketches.filters.bloomfilter.BloomFilterBuilder;

import java.util.ArrayList;

public class BloomFilterAccuracyProfile extends BaseFilterAccuracyProfile{

    protected BloomFilter sketch;

    protected long filterLengthBits;
//    protected int minNumBitsPerEntry;
//    protected int maxNumBitsPerEntry ;

    @Override
    public void configure() {
        filterLengthBits = Integer.parseInt(prop.mustGet("filterLengthBits"));
    }

    /*
     * The experiment hasa fixed number of bits per entry and
     */
    @Override
    public double doTrial(final int  numHashes, final long numQueries) {

        // Initialise and populate the sketch
        filterLengthBits = (long) ((numHashes * numItemsInserted) / LN2);
        sketch =  BloomFilterBuilder.createBySize(filterLengthBits, numHashes);

        // Build the test sets but clear them first so that they have the correct cardinality and no surplus is added.
        inputItems.clear();
        negativeItems.clear();
        long item;
        for (long i = 0; i < numItemsInserted; i++){
            item = ++vIn;
            inputItems.add(item) ;
            sketch.update(item);
        }

        for (long i = numItemsInserted; i < numItemsInserted+numQueries; i++ ) {
            item = ++vIn;
            negativeItems.add(item) ;
        }

        // Check the number of false positives
        long numFalsePositive = 0 ;
        for (int i = 0; i < negativeItems.size(); i++){
            if (sketch.query(negativeItems.get(i))) ++numFalsePositive ;
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
