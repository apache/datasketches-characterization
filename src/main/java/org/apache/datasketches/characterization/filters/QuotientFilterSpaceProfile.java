package org.apache.datasketches.characterization.filters;

import org.apache.datasketches.filters.quotientfilter.QuotientFilter;
import org.apache.datasketches.filters.quotientfilter.QuotientFilterBuilder;

import static org.apache.datasketches.common.Util.pwr2SeriesNext;

public class QuotientFilterSizeProfile extends BaseSizeProfile {
    protected QuotientFilter sketch;

    public void configure() {}

    @Override
    //public long doTrial(long inputCardinality, double targetFpp) {
    public trialResults doTrial(long inputCardinality, double targetFpp){
        trialResults result = new trialResults();
        int fingerprintLength = QuotientFilterBuilder.suggestFingerprintLength(targetFpp);
        int lgNumSlots = QuotientFilterBuilder.suggestLgNumSlots(inputCardinality); // Make sure to check this line if you want -1 or not
        sketch = new QuotientFilter(lgNumSlots, fingerprintLength+3);

        long numQueries = pwr2SeriesNext(1, 1L<<(fingerprintLength+1));
        // Build the test sets but clear them first so that they have the correct cardinality and no surplus is added.
        inputItems.clear();
        negativeItems.clear();
        long item;
        for (long i = 0; i < inputCardinality; i++){
            item = ++vIn;
            inputItems.add(item) ;
            sketch.insert(item);
        }

        for (long i = inputCardinality; i < inputCardinality+numQueries; i++ ) {
            item = ++vIn;
            negativeItems.add(item) ;
        }

        // Check the number of false positives
        long numFalsePositive = 0 ;
        for (int i = 0; i < negativeItems.size(); i++){
            if (sketch.search(negativeItems.get(i))) ++numFalsePositive ;
        }
        result.filterSizeBits =  sketch.getSpaceUse();
        result.measuredFalsePositiveRate = (double)numFalsePositive / numQueries;
        result.numHashbits = fingerprintLength;
        return result;
    }
}
