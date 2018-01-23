package com.yahoo.sketches.characterization.quantiles;

import static com.yahoo.sketches.Util.pwr2LawNext;

import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;

public abstract class QuantilesSpeedProfile implements JobProfile {

  private Job job;

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
    final int lgMinStreamLen = Integer.parseInt(job.getProperties().mustGet("lgMin"));
    final int lgMaxStreamLen = Integer.parseInt(job.getProperties().mustGet("lgMax"));
    final int minStreamLen = 1 << lgMinStreamLen;
    final int maxStreamLen = 1 << lgMaxStreamLen;
    final int pointsPerOctave = Integer.parseInt(job.getProperties().mustGet("PPO"));

    final int lgMaxTrials = Integer.parseInt(job.getProperties().mustGet("lgMaxTrials"));
    final int lgMinTrials = Integer.parseInt(job.getProperties().mustGet("lgMinTrials"));

    final int lgK = Integer.parseInt(job.getProperties().mustGet("lgK"));
    final int numQueryValues = Integer.parseInt(job.getProperties().mustGet("numQueryValues"));
    final boolean useDirect = Boolean.parseBoolean(job.getProperties().mustGet("useDirect"));

    configure(lgK, numQueryValues, useDirect);

    // header
    println("Stream\tTrials\tBuild\tUpdate\tQuant\tCDF\tRank"
        + "\tSer\tDeser\tCompact\tQuant\tCDF\tRank\tSer\tDeser");

    int streamLength = minStreamLen;
    while (streamLength <= maxStreamLen) {
      prepareTrial(streamLength);
      final SpeedStats stats = new SpeedStats();
      final int numTrials = getNumTrials(streamLength, lgMinStreamLen, lgMaxStreamLen,
          lgMinTrials, lgMaxTrials);
      for (int i = 0; i < numTrials; i++) {
        doTrial(stats);
      }
      println(String.format("%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f"
            + "\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f",
          streamLength,
          numTrials,
          (double) stats.buildTimeNs / numTrials,
          (double) stats.updateTimeNs / numTrials / streamLength,
          (double) stats.updateGetQuantilesTimeNs / numTrials / numQueryValues,
          (double) stats.updateGetCdfTimeNs / numTrials / numQueryValues,
          (double) stats.updateGetRankTimeNs / numTrials / numQueryValues,
          (double) stats.updateSerializeTimeNs / numTrials,
          (double) stats.updateDeserializeTimeNs / numTrials,
          (double) stats.compactTimeNs / numTrials,
          (double) stats.compactGetQuantilesTimeNs / numTrials / numQueryValues,
          (double) stats.compactGetCdfTimeNs / numTrials / numQueryValues,
          (double) stats.compactGetRankTimeNs / numTrials / numQueryValues,
          (double) stats.compactSerializeTimeNs / numTrials,
          (double) stats.compactDeserializeTimeNs / numTrials
      ));
      streamLength = pwr2LawNext(pointsPerOctave, streamLength);
    }
  }

  abstract void configure(int lgK, int numQueryValues, boolean useDirect);

  abstract void prepareTrial(int streamLength);

  abstract void doTrial(SpeedStats stats);

  static class SpeedStats {
    long buildTimeNs = 0;
    long updateTimeNs = 0;
    long updateGetQuantilesTimeNs = 0;
    long updateGetCdfTimeNs = 0;
    long updateGetRankTimeNs = 0;
    long updateSerializeTimeNs = 0;
    long updateDeserializeTimeNs = 0;
    long compactTimeNs = 0;
    long compactGetQuantilesTimeNs = 0;
    long compactGetCdfTimeNs = 0;
    long compactGetRankTimeNs = 0;
    long compactSerializeTimeNs = 0;
    long compactDeserializeTimeNs = 0;
  }

  private static int getNumTrials(final int x, final int lgMinX, final int lgMaxX,
      final int lgMinTrials, final int lgMaxTrials) {
    final double slope = (double) (lgMaxTrials - lgMinTrials) / (lgMinX - lgMaxX);
    final double lgX = Math.log(x) / JobProfile.LN2;
    final double lgTrials = (slope * lgX) + lgMaxTrials;
    return (int) Math.pow(2, lgTrials);
  }

}
