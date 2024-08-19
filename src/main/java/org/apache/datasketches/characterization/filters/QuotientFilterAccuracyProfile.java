package org.apache.datasketches.characterization.filters;

import org.apache.datasketches.filters.quotientfilter.QuotientFilter;
import org.apache.datasketches.filters.quotientfilter.QuotientFilterBuilder;

import java.util.ArrayList;


public class QuotientFilterAccuracyProfile extends BaseFilterAccuracyProfile{

    protected QuotientFilter sketch;


    @Override
    public void configure() {}

    /*
    We use the numHashBits as being equivalent to the number of bits per entry in the Quotient filter.
    This value is 3 metadata bits plus the remaining number of bits for the fingerprint length.
     */
    @Override
    public double doTrial(final int numHashBits, final long numQueries) {
        // Initialise and populate the sketch
        sketch = new QuotientFilter(lgU, numHashBits);

        // Build the test sets but clear them first so that they have the correct cardinality and no surplus is added.
        inputItems.clear();
        negativeItems.clear();
        long item;
        for (long i = 0; i < numItemsInserted; i++){
            item = ++vIn;
            inputItems.add(item) ;
            sketch.insert(item);
        }

        for (long i = numItemsInserted; i < numItemsInserted+numQueries; i++ ) {
            item = ++vIn;
            negativeItems.add(item) ;
        }

        // Check the number of false positives
        long numFalsePositive = 0 ;
        for (int i = 0; i < negativeItems.size(); i++){
            if (sketch.search(negativeItems.get(i))) ++numFalsePositive ;
        }
        return (double)numFalsePositive / numQueries;
    }

    /*
    The total number of bits per entry is the three metadata plus the fingerprint length bits.
    This is exactly the number of hash bits that is passed as a parameter.
     */
    @Override
    public int getBitsperEntry(final int numHashBits) {
        return numHashBits;
    }

    @Override
    public long getFilterLengthBits() {
        return sketch.getSpaceUse();
    }


}

