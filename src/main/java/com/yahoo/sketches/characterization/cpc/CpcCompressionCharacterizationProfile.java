/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.cpc;

import java.io.PrintStream;
import java.io.PrintWriter;

import com.yahoo.sketches.characterization.Job;
import com.yahoo.sketches.characterization.JobProfile;
import com.yahoo.sketches.characterization.Properties;
import com.yahoo.sketches.cpc.CompressionCharacterization;

/**
 * @author Lee Rhodes
 */
public class CpcCompressionCharacterizationProfile implements JobProfile {
  Job job;
  Properties prop;

  int lgMinK;
  int lgMaxK; //inclusive
  int lgMinT;
  int lgMaxT;
  int lgMulK;
  int uPPO;
  int incLgK;
  PrintStream ps;
  PrintWriter pw;

  @Override
  public void start(final Job job) {
    this.job = job;
    prop = job.getProperties();

    lgMinK = Integer.parseInt(prop.mustGet("lgMinK"));
    lgMaxK = Integer.parseInt(prop.mustGet("lgMaxK"));
    lgMaxT = Integer.parseInt(prop.mustGet("Trials_lgMaxT"));
    incLgK = Integer.parseInt(prop.mustGet("incLgK"));
    uPPO = Integer.parseInt(prop.mustGet("uPPO"));
    ps = System.out;
    pw = job.getPrintWriter();

    final CompressionCharacterization cc = new CompressionCharacterization(
        lgMinK, lgMaxK, lgMinT, lgMaxT, lgMulK, uPPO, incLgK, ps, pw);
    cc.start();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

  @Override
  public void println(final String s) {}

}
