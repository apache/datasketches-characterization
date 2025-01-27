package org.apache.datasketches.characterization.filters;
import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;

import java.util.ArrayList;

import static java.lang.Math.log;
import static java.lang.Math.pow;
import static org.apache.datasketches.common.Util.pwr2SeriesNext;

class trialResults{
    long filterSizeBits;
    double measuredFalsePositiveRate;

    int numHashbits;
}

public abstract class BaseSpaceProfile implements JobProfile{
    Job job;
    public Properties prop;
    double targetFpp;
    public long vIn = 0;
    int lgMinT;
    int lgMaxT;
    int tPPO;
    int lgMinU;
    int lgMaxU;
    int uPPO;
    long inputCardinality;
    public ArrayList<Long> inputItems = new ArrayList<>();
    public ArrayList<Long> negativeItems = new ArrayList<>();
    int numTrials;
    int lgMinBpU;
    int lgMaxBpU;
    double slope;


    @Override
    public void start(final Job job) {

        this.job = job;
        prop = job.getProperties();
        targetFpp = Double.parseDouble(prop.mustGet("targetFpp"));
        //Uniques Profile
        lgMinU = Integer.parseInt(prop.mustGet("Trials_lgMinU"));
        lgMaxU = Integer.parseInt(prop.mustGet("Trials_lgMaxU"));
        uPPO = Integer.parseInt(prop.mustGet("Trials_UPPO"));
        inputCardinality = (int)pwr2SeriesNext(uPPO, 1L<<lgMinU);

        //Trials Profile
        lgMinT = Integer.parseInt(prop.mustGet("Trials_lgMinT"));
        lgMaxT = Integer.parseInt(prop.mustGet("Trials_lgMaxT"));
        tPPO = Integer.parseInt(prop.mustGet("Trials_TPPO"));

        // Uniques profile
        lgMinBpU = Integer.parseInt(prop.mustGet("Trials_lgMinBpU"));
        lgMaxBpU = Integer.parseInt(prop.mustGet("Trials_lgMaxBpU"));
        slope = (double) (lgMaxT - lgMinT) / (lgMinBpU - lgMaxBpU);

        configure();
        doTrials();
        shutdown();
        cleanup();
    }

    @Override
    public void shutdown() {}

    @Override
    public void cleanup() {}

    public abstract void configure();

    // In here should have the logic to initialize a new sketch for a different number of input items for each trial.
    //public abstract long doTrial(long inputCardinality, double targetFpp);
    public abstract trialResults doTrial(long inputCardinality, double targetFpp);

    /*
     This experiment varies the cardinality of the input and measures the filter size required
     to obtain an input false positive rate.
     Inputs:
     - falsePositiveRate: the target false positive rate
     */
    private void doTrials() {
        final StringBuilder dataStr = new StringBuilder();
        final int minT = 1 << lgMinT;
        final int maxT = 1 << lgMaxT;
        final long maxU = 1L << lgMaxU;
        job.println(getHeader());

        while(inputCardinality < maxU) {
            numTrials = getNumTrials(inputCardinality);
            //doTrial(inputCardinality, targetFpp, final long numQueries);
            inputCardinality = (int)pwr2SeriesNext(uPPO, inputCardinality);
            //long filterNumBits = doTrial(inputCardinality, targetFpp) ;
            trialResults results =  doTrial(inputCardinality, targetFpp) ;
            process(inputCardinality, results.filterSizeBits, numTrials,
                    results.measuredFalsePositiveRate,  results.numHashbits, dataStr);
            job.println(dataStr.toString()) ;
        }
    }


    /**
     * Computes the number of trials for a given current number of uniques for a
     * trial set. This is used to decrease the number of trials
     * as the number of uniques increase.
     *
     * @param curU the given current number of uniques for a trial set.
     * @return the number of trials for a given current number of uniques for a
     * trial set.
     */
    private int getNumTrials(final long curU) {
        final int minBpU = 1 << lgMinBpU;
        final int maxBpU = 1 << lgMaxBpU;
        final int maxT = 1 << lgMaxT;
        final int minT = 1 << lgMinT;
        if (lgMinT == lgMaxT || curU <= minBpU) {
            return maxT;
        }
        if (curU >= maxBpU) {
            return minT;
        }
        final double lgCurU = log(curU) / LN2;
        final double lgTrials = slope * (lgCurU - lgMinBpU) + lgMaxT;
        return (int) pow(2.0, lgTrials);
    }

    /**
     * Processes the results of a trial and appends them to a StringBuilder in a tab-separated format.
     *
     * @param inputCardinality The number of hashes used in the trial.
     * @param sb The StringBuilder to which the results are appended.
     */
    private static void process(final long inputCardinality, final long sizeInBits, final long numTrials,
                                final double falsePositiveRate, final int numHashBits, final StringBuilder sb) {
        // OUTPUT
        sb.setLength(0);
        sb.append(inputCardinality).append(TAB);
        sb.append(sizeInBits).append(TAB);
        sb.append(numTrials).append(TAB);
        sb.append(falsePositiveRate).append(TAB);
        sb.append(numHashBits);
    }

    private String getHeader() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TrueU").append(TAB);
        sb.append("Size").append(TAB);
        sb.append("NumTrials").append(TAB);
        sb.append("FalsePositiveRate").append(TAB);
        sb.append("NumHashBits");
        return sb.toString();
    }

}
