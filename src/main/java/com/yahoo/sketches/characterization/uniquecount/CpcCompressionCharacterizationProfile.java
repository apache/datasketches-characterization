/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization.uniquecount;

import java.io.PrintStream;
import java.io.PrintWriter;

import com.yahoo.sketches.Job;
import com.yahoo.sketches.JobProfile;
import com.yahoo.sketches.Properties;
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
    lgMinT = Integer.parseInt(prop.mustGet("lgMinT"));
    lgMaxT = Integer.parseInt(prop.mustGet("lgMaxT"));
    lgMulK = Integer.parseInt(prop.mustGet("lgMulK"));
    uPPO = Integer.parseInt(prop.mustGet("uPPO"));
    incLgK = Integer.parseInt(prop.mustGet("incLgK"));

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
