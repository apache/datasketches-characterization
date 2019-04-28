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
import com.yahoo.sketches.cpc.StreamingValidation;

/**
 * @author Lee Rhodes
 */
public class CpcStreamingValidationProfile implements JobProfile {
  Job job;
  Properties prop;

  int lgMinK; //For each K to
  int lgMaxK;
  int trials;
  int ppoN;
  PrintStream ps;
  PrintWriter pw;

  @Override
  public void start(final Job job) {
    this.job = job;
    pw = job.getPrintWriter();
    ps = System.out;
    prop = job.getProperties();
    lgMinK = Integer.parseInt(prop.mustGet("lgMinK"));
    lgMaxK = Integer.parseInt(prop.mustGet("lgMaxK"));
    trials = Integer.parseInt(prop.mustGet("trials"));
    ppoN = Integer.parseInt(prop.mustGet("ppoN"));
    final StreamingValidation sVal = new StreamingValidation(lgMinK, lgMaxK, trials, ppoN, ps, pw);
    sVal.start();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

  @Override
  public void println(final String s) {}

}
