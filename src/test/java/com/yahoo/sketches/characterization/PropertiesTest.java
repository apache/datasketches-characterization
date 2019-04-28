/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.yahoo.sketches.Properties;

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
   *
   * @param s string to print
   */
  static void println(String s) {
    //System.out.println(s); //Disable here
  }

}
