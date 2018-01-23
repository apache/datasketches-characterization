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
    final int lgMin = Integer.parseInt(job.getProperties().mustGet("lgMin"));
    final int lgMax = Integer.parseInt(job.getProperties().mustGet("lgMax"));
    final int ppo = Integer.parseInt(job.getProperties().mustGet("PPO"));
    final int numTrials = Integer.parseInt(job.getProperties().mustGet("trials"));

    configure(job.getProperties());

    job.println("StreamLength\tMaxError");

    final int numSteps = PerformanceUtil.countPoints(lgMin, lgMax, ppo);
    int streamLength = 1 << lgMin;
    for (int i = 0; i < numSteps; i++) {
      prepareTrial(streamLength);
      double maxError = 0;
      for (int t = 0; t < numTrials; t++) {
        maxError = Math.max(maxError, doTrial());
      }
      println(streamLength + "\t" + String.format("%.2f", maxError * 100));
      streamLength = pwr2LawNext(ppo, streamLength);
    }
  }

  abstract void configure(Properties props);

  abstract void prepareTrial(int streamLength);

  abstract double doTrial();

}
