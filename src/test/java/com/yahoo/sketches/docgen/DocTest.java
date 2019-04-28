/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.docgen;

import org.testng.annotations.Test;

import com.yahoo.sketches.docgen.Doc;

/**
 * @author Lee Rhodes
 */
public class DocTest {

  @Test
  public void checkDoc() {
    Doc doc = new Doc("Frequency", "FrequentItemsOverview", "Frequent Items Overview");
    println(doc.toString());
  }
  static void println(String s) { System.out.println(s); }
}
