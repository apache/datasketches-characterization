/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.characterization.cpc;

import java.io.PrintStream;
import java.io.PrintWriter;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;
import org.apache.datasketches.cpc.CompressionCharacterization;

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
