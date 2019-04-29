/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.docgen;

import java.util.Iterator;
import java.util.List;

/**
 * @author Lee Rhodes
 */
public class Dropdown {
  private static final String LS = System.getProperty("line.separator");
  String desc;
  String pId;
  String divId;
  String href;
  List<Object> list;
  int level;  //starting with 1;

  /**
   * A Drop down elememt.
   * @param desc the description
   * @param list the list of documents or dropdowns to contain
   * @param level the current level of this dropdown
   */
  public Dropdown(final String desc, final List<Object> list, final int level) {
    this.desc = desc;
    this.list = list;
    final String lowercaseDesc = desc.toLowerCase();
    pId = lowercaseDesc.replace(' ', '-');
    divId = "collapse_" + lowercaseDesc.replace(' ', '_');
    href = "#" + divId;
    this.level = level;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    final String indent = indent(level);
    //paragraph with desc
    sb.append(indent).append("<p id=").append(quotes(pId)).append(LS);
    sb.append(indent).append("  ").append("<a data-toggle=\"collapse\" ")
      .append("class=\"menu collapsed\" href=").append(quotes(href)).append(">")
      .append(desc).append("</a>").append(LS);
    sb.append(indent).append("</p>").append(LS);
    //dropdown list
    sb.append(indent).append("<div class=\"collapse\" ").append("id=").append(quotes(divId))
      .append(">").append(LS);
    //list of docs
    sb.append(indent).append("  LIST OF DOCS").append(LS);
    sb.append(indent).append("</div>").append(LS);
    return sb.toString();
  }

  private static String outputList(final List<Object> list) {
    final Iterator<Object> itr = list.iterator();
    final StringBuilder sb = new StringBuilder();
    while (itr.hasNext()) {
      final Object next = itr.next();
      if (next instanceof Dropdown) {
        final Dropdown dd = (Dropdown) next;
        //
      } else {
        final Doc doc = (Doc) next;
        //
      }
    }
    return sb.toString();
  }



  public static String quotes(final String s) {
    return '"' + s + '"';
  }

  /**
   *
   * @param level indention level
   * @return the indention spaces
   */
  public static String indent(final int level) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < level; i++) {
      sb.append("  ");
    }
    return sb.toString();
  }

}
