package org.apache.datasketches.characterization.quantiles.tdigest;

import static org.apache.datasketches.Util.pwr2LawNext;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.PerformanceUtil;
import org.apache.datasketches.Properties;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketchBuilder;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

public abstract class QuantilesAccuracyProfile implements JobProfile {

  Job job;
  private DoublesSketchBuilder builder;

  @Override
  public void start(final Job job) {
    this.job = job;
    doTrials();
  }

  @Override
  public void println(final Object o) {
    job.println(o.toString());
  }

  private void doTrials() {
    final int lgMin = Integer.parseInt(job.getProperties().mustGet("lgMin"));
    final int lgMax = Integer.parseInt(job.getProperties().mustGet("lgMax"));
    final int ppo = Integer.parseInt(job.getProperties().mustGet("PPO"));
    final int numTrials = Integer.parseInt(job.getProperties().mustGet("trials"));

    final int errorSketchLgK = Integer.parseInt(job.getProperties().mustGet("errLgK"));
    final int errorPct = Integer.parseInt(job.getProperties().mustGet("errPct"));

    builder = DoublesSketch.builder().setK(1 << errorSketchLgK);

    configure(job.getProperties());

    job.println("StreamLength\tError");

    final int numSteps = PerformanceUtil.countPoints(lgMin, lgMax, ppo);
    int streamLength = 1 << lgMin;
    for (int i = 0; i < numSteps; i++) {
      prepareTrial(streamLength);
      final UpdateDoublesSketch rankErrorSketch = builder.build();
      for (int t = 0; t < numTrials; t++) {
        final double maxRankErrorInTrial = doTrial();
        rankErrorSketch.update(maxRankErrorInTrial);
      }
      println(streamLength + "\t"
          + String.format("%.2f", rankErrorSketch.getQuantile((double) errorPct / 100) * 100));
      streamLength = pwr2LawNext(ppo, streamLength);
    }
  }

  abstract void configure(Properties props);

  abstract void prepareTrial(int streamLength);

  abstract double doTrial();

}
