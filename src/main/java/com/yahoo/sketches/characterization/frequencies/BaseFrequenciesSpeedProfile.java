package com.yahoo.sketches.characterization.frequencies;

import static com.yahoo.sketches.Util.pwr2LawNext;

import com.yahoo.sketches.Job;
import com.yahoo.sketches.JobProfile;
import com.yahoo.sketches.Properties;

public abstract class BaseFrequenciesSpeedProfile implements JobProfile {

  private Job job;

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

  private void doTrials() {
    final int lgMinStreamLen = Integer.parseInt(job.getProperties().mustGet("lgMin"));
    final int lgMaxStreamLen = Integer.parseInt(job.getProperties().mustGet("lgMax"));
    final int minStreamLen = 1 << lgMinStreamLen;
    final int maxStreamLen = 1 << lgMaxStreamLen;
    final int pointsPerOctave = Integer.parseInt(job.getProperties().mustGet("PPO"));

    final int lgMaxTrials = Integer.parseInt(job.getProperties().mustGet("lgMaxTrials"));
    final int lgMinTrials = Integer.parseInt(job.getProperties().mustGet("lgMinTrials"));

    configure(job.getProperties());

    println(getHeader());

    int streamLength = minStreamLen;
    while (streamLength <= maxStreamLen) {
      final int numTrials = getNumTrials(streamLength, lgMinStreamLen, lgMaxStreamLen, lgMinTrials,
          lgMaxTrials);
      resetStats();
      for (int i = 0; i < numTrials; i++) {
        prepareTrial(streamLength);
        doTrial();
      }
      println(getStats(streamLength, numTrials));
      streamLength = pwr2LawNext(pointsPerOctave, streamLength);
    }
  }

  abstract void configure(Properties properties);

  abstract void prepareTrial(int streamLength);

  abstract void doTrial();

  abstract String getHeader();

  abstract String getStats(int streamLength, int numTrials);

  abstract void resetStats();

  private static int getNumTrials(final int x, final int lgMinX, final int lgMaxX,
      final int lgMinTrials, final int lgMaxTrials) {
    final double slope = (double) (lgMaxTrials - lgMinTrials) / (lgMinX - lgMaxX);
    final double lgX = Math.log(x) / JobProfile.LN2;
    final double lgTrials = (slope * lgX) + lgMaxTrials;
    return (int) Math.pow(2, lgTrials);
  }

}
