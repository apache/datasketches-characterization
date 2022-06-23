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

import org.apache.datasketches.Family;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.characterization.AccuracyStats;
import org.apache.datasketches.characterization.uniquecount.BaseAccuracyProfile;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;

public class ThetaAccuracyIntersectionProfile extends BaseAccuracyProfile {
  private int myLgK; //avoids temporary conflict with BaseAccuracyProfile
  private boolean rebuild;
  private UpdateSketch skSm;
  private UpdateSketch skLg;
  private Intersection intersection;

  @Override
  public void configure() {
    //Theta Sketch Profile
    myLgK = Integer.parseInt(prop.mustGet("LgK"));
    rebuild = Boolean.parseBoolean(prop.mustGet("Rebuild"));
    final Family family = Family.stringToFamily(prop.mustGet("THETA_famName"));
    final ResizeFactor rf = ResizeFactor.getRF(Integer.parseInt(prop.mustGet("LgRF")));
    final float p = Float.parseFloat(prop.mustGet("P"));
    //final boolean direct = Boolean.parseBoolean(prop.mustGet("Direct"));
    final UpdateSketchBuilder udBldr = new UpdateSketchBuilder()
      .setLogNominalEntries(myLgK)
      .setFamily(family)
      .setP(p)
      .setResizeFactor(rf);
    skSm = udBldr.build();
    skLg = udBldr.build();
    final SetOperationBuilder soBldr = new SetOperationBuilder()
        .setLogNominalEntries(myLgK);
    intersection = soBldr.buildIntersection();
  }

  @Override
  public void doTrial() {
    final int qArrLen = qArr.length;
    skSm.reset();
    skLg.reset();
    long lastUniques = 0;
    for (int i = 0; i < qArrLen; i++) {
      final AccuracyStats q = qArr[i];
      final long delta = (long)(q.trueValue - lastUniques);
      for (long u = 0; u < delta; u++) {
        if (i == 0) { skSm.update(vIn); }
        skLg.update(vIn++);
      }
      lastUniques += delta;
      if (rebuild) {
        skSm.rebuild();
        skLg.rebuild();
      }
      final double est = intersection.intersect(skLg, skSm).getEstimate();
      q.update(est);
    }
  }

}

