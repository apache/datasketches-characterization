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

package org.apache.datasketches;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import org.apache.datasketches.Properties;

/**
 * @author Lee Rhodes
 */
public class PropertiesTest {

  @Test
  public void checkLoadKvPairs() {
    Properties prop = new Properties();
    String s = "k1=v1,k2=v2\tk3=v3\nk4=v4";
    prop.loadKvPairs(s);
    String out = prop.extractKvPairs();
    println(out);
    prop = new Properties();
    prop.loadKvPairs(out);
    out = prop.extractKvPairs();
    println(out);
  }

  @Test
  public void checkEmptyReturn() {
    Properties prop = new Properties();
    String s = prop.extractKvPairs();
    assertTrue(s.isEmpty());
  }

  @Test
  public void checkSpaceRemoval() {
    Properties prop = new Properties();
    String s = " key  =  value  ";
    prop.loadKvPairs(s);
    String out = prop.extractKvPairs();
    println(out);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void checkMissingKVSeparator() {
    Properties prop = new Properties();
    String s = " key  ;  value  ";
    prop.loadKvPairs(s);
  }

  /**
   * @param s string to print
   */
  static void println(final String s) {
    //System.out.println(s); //Disable here
  }

}
