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
import com.yahoo.sketches.cpc.MergingValidation;

/**
 * @author Lee Rhodes
 */
public class CpcMergingValidationProfile implements JobProfile {
  Job job;
  Properties prop;

  int lgMinK;
  int lgMaxK; //inclusive
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
    lgMulK = Integer.parseInt(prop.mustGet("lgMulK"));
    uPPO = Integer.parseInt(prop.mustGet("uPPO"));
    incLgK = Integer.parseInt(prop.mustGet("incLgK"));

    ps = System.out;
    pw = job.getPrintWriter();

    final MergingValidation mv = new MergingValidation(
        lgMinK, lgMaxK, lgMulK, uPPO, incLgK, ps, pw);
    mv.start();
  }

  @Override
  public void shutdown() {}

  @Override
  public void cleanup() {}

  @Override
  public void println(final String s) {}

}
