package com.yahoo.sketches.characterization.quantiles;

import static com.yahoo.sketches.Util.pwr2LawNext;

import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.characterization.Properties;

public abstract class QuantilesSpeedProfile implements JobProfile {

  private Job job;

  //JobProfile
  @Override
  public void start(final Job job) {
    this.job = job;
    doTrials();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

  @Override
  public void println(final String s) {
    job.println(s);
  }
  //end JobProfile

  private void doTrials() {
    final int lgMinStreamLen = Integer.parseInt(job.getProperties().mustGet("lgMin"));
    final int lgMaxStreamLen = Integer.parseInt(job.getProperties().mustGet("lgMax"));
    final int minStreamLen = 1 << lgMinStreamLen;
    final int maxStreamLen = 1 << lgMaxStreamLen;
    final int pointsPerOctave = Integer.parseInt(job.getProperties().mustGet("PPO"));

    final int lgMaxTrials = Integer.parseInt(job.getProperties().mustGet("lgMaxTrials"));
    final int lgMinTrials = Integer.parseInt(job.getProperties().mustGet("lgMinTrials"));

    final int k = Integer.parseInt(job.getProperties().mustGet("K"));
    final int numQueryValues = Integer.parseInt(job.getProperties().mustGet("numQueryValues"));

    configure(k, numQueryValues, job.getProperties());

    println(getHeader());

    int streamLength = minStreamLen;
    while (streamLength <= maxStreamLen) {
      prepareTrial(streamLength);
      final int numTrials = getNumTrials(streamLength, lgMinStreamLen, lgMaxStreamLen,
          lgMinTrials, lgMaxTrials);
      for (int i = 0; i < numTrials; i++) {
        doTrial();
      }
      println(getStats(streamLength, numTrials, numQueryValues));
      streamLength = pwr2LawNext(pointsPerOctave, streamLength);
    }
  }

  abstract void configure(int k, int numQueryValues, Properties properties);

  abstract void prepareTrial(int streamLength);

  abstract void doTrial();

  abstract String getHeader();

  abstract String getStats(int streamLength, int numTrials, int numQueryValues);

  private static int getNumTrials(final int x, final int lgMinX, final int lgMaxX,
      final int lgMinTrials, final int lgMaxTrials) {
    final double slope = (double) (lgMaxTrials - lgMinTrials) / (lgMinX - lgMaxX);
    final double lgX = Math.log(x) / JobProfile.LN2;
    final double lgTrials = (slope * lgX) + lgMaxTrials;
    return (int) Math.pow(2, lgTrials);
  }

}
