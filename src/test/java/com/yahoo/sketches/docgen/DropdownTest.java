/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.docgen;

import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class DropdownTest {

  @Test
  public void checkDropdown() {
    Dropdown dropdn = new Dropdown("The Description", null, 1);
    println(dropdn.toString());
  }

  static void println(String s) { System.out.println(s); }
}
