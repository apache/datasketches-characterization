package org.apache.datasketches.characterization.filters;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;

import java.util.ArrayList;

import static java.lang.Math.log;
import static java.lang.Math.pow;
import static org.apache.datasketches.common.Util.pwr2SeriesNext;

public abstract class BaseFilterAccuracyProfile implements JobProfile{

    Job job;
    public Properties prop;
    public long vIn = 1;
    public int lgU;
    public double capacity;
    public long numItemsInserted;
    public ArrayList<Long> inputItems = new ArrayList<>();
    public ArrayList<Long> negativeItems = new ArrayList<>();
    long numQueries;

    int minNumHashes;
    int maxNumHashes ;
    int bitsPerEntry;
    int lgMinT;
    int lgMaxT;
    int tPPO;
    int numTrials ;
    double fpr;
    long filterNumBits;
    int lgMinBpU;
    int lgMaxBpU;
    double slope;

    //JobProfile
    @Override
    public void start(final Job job) {
        this.job = job;
        prop = job.getProperties();
        lgU = Integer.parseInt(prop.mustGet("Universe_lgU"));
        capacity = Double.parseDouble(prop.mustGet("Universe_capacity"));
        minNumHashes = Integer.parseInt(prop.mustGet("minNumHashes"));
        maxNumHashes = Integer.parseInt(prop.mustGet("maxNumHashes"));
        lgMinT = Integer.parseInt(prop.mustGet("Trials_lgMinT"));
        lgMaxT = Integer.parseInt(prop.mustGet("Trials_lgMaxT"));
        lgMinBpU = Integer.parseInt(prop.mustGet("Trials_lgMinBpU"));
        lgMaxBpU = Integer.parseInt(prop.mustGet("Trials_lgMaxBpU"));
        slope = (double) (lgMaxT - lgMinT) / (lgMinBpU - lgMaxBpU);
        tPPO = Integer.parseInt(prop.mustGet("Trials_TPPO"));
        numQueries = 1<<minNumHashes+1; // starting value for the number of query points.
        numTrials = 1<<lgMinT;
        numItemsInserted = (long) Math.round(capacity * (1L << lgU));

        configure();
        doTrials();
        shutdown();
        cleanup();
    }

    @Override
    public void shutdown() {}

    @Override
    public void cleanup() {}
    //end JobProfile

    /**
     * Configure the sketch
     */
    public abstract void configure();

    /**
     * Used to get the size of the filter in bits for the current trial.
     */
    public abstract long getFilterLengthBits();


    /**
     * @return the number of bits per entry for the filter.
     */
    public abstract int getBitsperEntry(final int numHashes);

    /**
     * This method is used to perform a trial with a specific number of hashes and queries.
     *
     * @param numHashBits The number of hashes to be used in the trial.
     * See the specific profile for how this is implemented as the number of hashes
     * might have different meanings depending on the implementation.
     * @param numQueries The number of queries to be performed in the trial.
     * @return The average update time per update for this trial.
     */
    public abstract double doTrial(final int numHashBits, final long numQueries);

    /**
     * Conducts a series of trials for different numbers of hashes. For each number of hashes,
     * it performs a set number of trials, calculates the false positive rate and filter size,
     * and processes the results. The results of each trial are appended to a StringBuilder
     * in a tab-separated format and printed.
     *
     * The number of hashes ranges from the minimum to the maximum number of hashes specified
     * in the class. The number of trials for each number of hashes is determined by the
     * getNumTrials method. The false positive rate and filter size are calculated by
     * averaging the results of the trials for each number of hashes.
     *
     * After the results are processed, they are printed and the number of query points is
     * updated for the next number of hashes.
     * We need to increase the power of 2 for each trial set because the failure probability decays
     * with 2^(-x) when x is related to the number of bits per entry.
     */
    private void doTrials() {
        final StringBuilder dataStr = new StringBuilder();
        job.println(getHeader());
        for (int nh= minNumHashes; nh <= maxNumHashes; nh++) {
            fpr = 0;
            filterNumBits = 0;
            final int numTrials = getNumTrials(nh);
            for (int t = 0; t < numTrials; t++) {
                fpr += doTrial(nh, numQueries);
                filterNumBits += getFilterLengthBits();
            }
            fpr /= numTrials;
            filterNumBits /= numTrials;
            //bitsPerEntry = getBitsperEntry(nh);
            process(nh, fpr, filterNumBits, numQueries, numTrials, dataStr);
            job.println(dataStr.toString());
            numQueries = (int)pwr2SeriesNext(1, 1L<<(nh+1));
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
    private int getNumTrials(final int curU) {
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
     * @param numHashes The number of hashes used in the trial.
     * @param falsePositiveRate The false positive rate observed in the trial.
     * @param filterSizeBits The size of the filter used in the trial, in bits.
     * @param numQueryPoints The number of query points used in the trial.
     * @param numTrials The number of trials conducted.
     * @param sb The StringBuilder to which the results are appended.
     */
    private static void process(final int numHashes, final double falsePositiveRate,
                                final long filterSizeBits, final long numQueryPoints,
                                final long numTrials,  final StringBuilder sb) {
        // OUTPUT
        sb.setLength(0);
        sb.append(numHashes).append(TAB);
        sb.append(String.format("%.5e", falsePositiveRate)).append(TAB);
        sb.append(filterSizeBits).append(TAB);
        sb.append(numQueryPoints).append(TAB);
        sb.append(numTrials);
    }

    /**
     * Returns a column header row
     * @return a column header row
     */
    private String getHeader() {
        final StringBuilder sb = new StringBuilder();
        sb.append("numHashes").append(TAB);
        sb.append("FPR").append(TAB);
        sb.append("filterSizeBits").append(TAB);
        sb.append("numQueryPoints").append(TAB);
        sb.append("numTrials");
        return sb.toString();
    }
}


