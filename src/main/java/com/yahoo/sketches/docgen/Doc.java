/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.docgen;

/**
 * @author Lee Rhodes
 */
public class Doc {
  private static final String LS = System.getProperty("line.separator");
  String dir = "/";
  String file;
  String desc;
  int level;

  /**
   *
   * @param dir the directory without pre or post "/"
   * @param file simple file name with no suffix
   * @param desc the description
   * @param level the indention level starts with 1.
   */
  public Doc(final String dir, final String file, final String desc, final int level) {
    this.dir = (dir == null) ? "/" : "/" + dir + "/";
    this.file = file;
    this.desc = desc;
    this.level = level;
  }

  @Override
  public String toString() {
    final String indent = Dropdown.indent(level);
    final StringBuilder sb = new StringBuilder();
    sb.append(indent).append("<li><a href=\"{{site.docs_dir}}")
      .append(dir)
      .append(file)
      .append(".html\">")
      .append(desc)
      .append("</a></li>").append(LS);
    return sb.toString();
  }
}
