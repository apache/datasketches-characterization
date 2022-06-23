/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.characterization.theta;

import org.apache.datasketches.Job;
import org.apache.datasketches.JobProfile;
import org.apache.datasketches.Properties;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;

public class ThetaAccuracyIntersectionVsIEProfile implements JobProfile {
  final static String LS = System.getProperty("line.separator");
  Job job;
  Properties prop;
  long vIn = 0;
  int lgK;
  ResizeFactor rf;
  float p;
  boolean direct;
  boolean rebuild;

  int trials;
  int minLg10U;
  int maxLg10U;
  int ppom;

  UpdateSketch skSm;
  UpdateSketch skLg;
  Intersection intersection;
  Union union;

  int plotPoints;
  long[] uPoints;
  double[] fArr;
  double[] yTheorThetaREArr; // sqrt(F) * 1/sqrt(k) //plot of theoretical Theta RE
  double[] yTheorIE_REArr;     // F * 1/sqrt(k) //plot of theoretical IE RE
  double[] yThetaREArr;    //plot of measured Theta RE
  double[] yIE_REArr;      //plot of meausred IE RE

  double[] yThetaRESumArr;
  double[] yIE_RESumArr;

  @Override
  public void start(final Job job) {
    this.job = job;
    prop = job.getProperties();
    lgK = Integer.parseInt(prop.mustGet("LgK"));
    rf = ResizeFactor.getRF(Integer.parseInt(prop.mustGet("LgRF")));
    p = Float.parseFloat(prop.mustGet("P"));
    direct = Boolean.parseBoolean(prop.mustGet("Direct"));
    rebuild = Boolean.parseBoolean(prop.mustGet("Rebuild"));

    trials = Integer.parseInt(prop.mustGet("Trials"));

    minLg10U = Integer.parseInt(prop.mustGet("MinLg10U"));
    maxLg10U = Integer.parseInt(prop.mustGet("MaxLg10U"));
    ppom = Integer.parseInt(prop.mustGet("PPOM"));
    configure();
    doTrials();
    process();
    shutdown();
    cleanup();
    job.println("");
  }

  void configure() {
    final double theorRSE = 1.0 / Math.sqrt(1 << lgK);
    plotPoints = (maxLg10U - minLg10U) * ppom + 1;
    fArr = new double[plotPoints];
    uPoints = new long[plotPoints];
    yTheorThetaREArr = new double[plotPoints];
    yTheorIE_REArr = new double[plotPoints];
    yThetaREArr = new double[plotPoints];
    yIE_REArr = new double[plotPoints];

    yThetaRESumArr = new double[plotPoints];
    yIE_RESumArr = new double[plotPoints];

    final double inc = 1.0 / ppom;
    for (int i = 0; i < plotPoints; i++) {
      final long aUb = (long)Math.pow(10, minLg10U + (i * inc));
      uPoints[i] = aUb;
      final long aIb = uPoints[0];
      final double F = (double)aUb / aIb;
      fArr[i] = F;
      yTheorIE_REArr[i] = F * theorRSE;
      yTheorThetaREArr[i] = Math.sqrt(F) * theorRSE;
    }
    configureSketches();
  }

  void configureSketches() {
    final UpdateSketchBuilder udBldr = new UpdateSketchBuilder()
      .setLogNominalEntries(lgK)
      .setP(p)
      .setResizeFactor(rf);
    skSm = udBldr.build();
    skLg = udBldr.build();
    final SetOperationBuilder soBldr = new SetOperationBuilder()
        .setLogNominalEntries(lgK);
    intersection = soBldr.buildIntersection();
    union = soBldr.buildUnion();
  }

  void doTrials() {
    for (int t = 1; t <= trials; t++) {
      doTrial();
    }
    for (int pp = 0; pp < plotPoints; pp++) {
      yThetaREArr[pp] = yThetaRESumArr[pp] / trials;
      yIE_REArr[pp] = yIE_RESumArr[pp] / trials;
    }
  }

  void doTrial() {
    final long base = uPoints[0];
    for (long u = 0; u < base; u++) {
      skSm.update(vIn);
      skLg.update(vIn++);
    }
    skSm.rebuild();
    skLg.rebuild();
    yThetaRESumArr[0] += (intersection.intersect(skLg, skSm).getEstimate() / base - 1.0) * 2.0;
    yIE_RESumArr[0] +=
        ((skSm.getEstimate() + skLg.getEstimate() - union.union(skSm, skLg).getEstimate()) / base - 1.0) * 2.0;

    for (int pp = 1; pp < plotPoints; pp++) {
      final long delta = uPoints[pp] - uPoints[pp - 1];
      for (long u = 0; u < delta; u++) {
        skLg.update(vIn++);
      }
      skLg.rebuild();
      yThetaRESumArr[pp] += intersection.intersect(skLg, skSm).getEstimate() / base - 1.0;
      yIE_RESumArr[pp] += (skSm.getEstimate() + skLg.getEstimate() - union.union(skSm, skLg).getEstimate())
          / base - 1.0;
    }
    skSm.reset();
    skLg.reset();
  }

  final String[] hdr = {"PP","F", "TheorThetaRE", "TheorIE_RE", "ThetaRE", "IE_RE"};
  final String hfmt  = "%6s %12s %12s %12s %12s %12s" + LS;
  final String fmt   = "%6d %12.3e %12.6f %12.6f %12.6f %12.6f" + LS;

  void process() {
    job.println("");
    job.printf(hfmt, (Object[]) hdr );
    for (int i = 0; i < plotPoints; i++) {
      job.printf(fmt, i, fArr[i], yTheorThetaREArr[i], yTheorIE_REArr[i], yThetaREArr[i], yIE_REArr[i]);
    }
  }

  @Override
  public void shutdown() { }

  @Override
  public void cleanup() { }

}

