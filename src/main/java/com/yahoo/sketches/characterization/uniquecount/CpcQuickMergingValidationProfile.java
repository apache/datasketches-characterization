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
import com.yahoo.sketches.cpc.QuickMergingValidation;

/**
 * @author Lee Rhodes
 */
public class CpcQuickMergingValidationProfile implements JobProfile {

  Job job;
  Properties prop;

  int lgMinK;
  int lgMaxK; //inclusive
  int incLgK;
  PrintStream ps;
  PrintWriter pw;

  @Override
  public void start(final Job job) {
    this.job = job;
    prop = job.getProperties();

    lgMinK = Integer.parseInt(prop.mustGet("lgMinK"));
    lgMaxK = Integer.parseInt(prop.mustGet("lgMaxK"));
    incLgK = Integer.parseInt(prop.mustGet("incLgK"));

    ps = System.out;
    pw = job.getPrintWriter();

    final QuickMergingValidation mv = new QuickMergingValidation(
        lgMinK, lgMaxK, incLgK, ps, pw);
    mv.start();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

  @Override
  public void println(final String s) {}
}
