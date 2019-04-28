/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.docgen;

/**
 * @author Lee Rhodes
 */
public class Doc {
  String dir = "/";
  String file;
  String desc;

  /**
   *
   * @param dir the directory without pre or post "/"
   * @param file simple file desc with no suffix
   * @param desc the description
   */
  public Doc(final String dir, final String file, final String desc) {
    this.dir = (dir == null) ? "/" : "/" + dir + "/";
    this.file = file;
    this.desc = desc;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("<li><a href=\"{{site.docs_dir}}");
    sb.append(dir);
    sb.append(file);
    sb.append(".html\">");
    sb.append(desc);
    sb.append("</a></li>");
    return sb.toString();
  }
}
