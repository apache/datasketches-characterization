package com.yahoo.sketches.characterization.quantiles;

import static com.yahoo.sketches.Util.pwr2LawNext;

import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.characterization.PerformanceUtil;
import com.yahoo.sketches.characterization.Properties;

public abstract class QuantilesAccuracyProfile implements JobProfile {

  Job job;

  @Override
  public void start(final Job job) {
    this.job = job;
    doTrials();
  }

  @Override
  public void println(final String s) {
    job.println(s);
  }

  private void doTrials() {
    final int lgMinU = Integer.parseInt(job.getProperties().mustGet("lgMin"));
    final int lgMaxU = Integer.parseInt(job.getProperties().mustGet("lgMax"));
    final int uPPO = Integer.parseInt(job.getProperties().mustGet("UPPO"));
    final int numTrials = Integer.parseInt(job.getProperties().mustGet("trials"));

    configure(job.getProperties());

    job.println("StreamLength\tMaxError");

    final int numSteps = PerformanceUtil.countPoints(lgMinU, lgMaxU, uPPO);
    int streamLength = 1 << lgMinU;
    for (int i = 0; i < numSteps; i++) {
      prepareTrial(streamLength);
      double maxError = 0;
      for (int t = 0; t < numTrials; t++) {
        maxError = Math.max(maxError, doTrial());
      }
      println(streamLength + "\t" + String.format("%.2f", maxError * 100));
      streamLength = pwr2LawNext(uPPO, streamLength);
    }
  }

  abstract void configure(Properties props);

  abstract void prepareTrial(int streamLength);

  abstract double doTrial();

}
