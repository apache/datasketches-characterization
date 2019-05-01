/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.docgen;

import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.annotations.Test;

import com.yahoo.sketches.Files;

/**
 * @author Lee Rhodes
 */
public class TocGenerator {
  static final String LS = System.getProperty("line.separator");
  int level = 0;

  public TocGenerator() {}

  @Test
  public void readJson() {
    final StringBuilder sb = new StringBuilder();
    final String jin = Files.fileToString("src/main/resources/test.txt");
    final JSONObject jo = new JSONObject(jin);
    final String clazz = jo.getString("class");
    if (!clazz.equals("TOC")) {
      throw new IllegalArgumentException("First class must be TOC");
    }
    else { emitToc(jo, sb); }
    println(sb.toString());
  }

  void emitToc(final JSONObject toc, final StringBuilder sb) {
    sb.append("<link rel=\"stylesheet\" href=\"/css/toc.css\">").append(LS);
    sb.append("<div id=\"toc\" class=\"nav toc hidden-print\">").append(LS);

    //JSONArray
    level++;
    final JSONArray jarr = toc.getJSONArray("array");
    final Iterator<Object> itr = jarr.iterator();
    while (itr.hasNext()) {
      final JSONObject jo = (JSONObject) itr.next();
      final String clazz = jo.getString("class");
      if (clazz.equals("Dropdown")) { emitDropdown(jo, sb); }
      else { emitDoc(jo, sb); }
    }
    level--;

    sb.append("</div>").append(LS);
  }

  void emitDropdown(final JSONObject dropdn, final StringBuilder sb) {
    final String desc = dropdn.getString("desc");
    final String lowercaseDesc = desc.toLowerCase();
    final String pId = lowercaseDesc.replace(' ', '-');
    final String divId = "collapse_" + lowercaseDesc.replace(' ', '_');
    final String href = "#" + divId;
    final String indent = indent(level);
    //paragraph with desc
    sb.append(LS);
    sb.append(indent).append("<p id=").append(quotes(pId)).append(LS);
    sb.append(indent).append("  ").append("<a data-toggle=\"collapse\" ")
      .append("class=\"menu collapsed\" href=").append(quotes(href)).append(">")
      .append(desc).append("</a>").append(LS);
    sb.append(indent).append("</p>").append(LS);
    //start dropdown array
    sb.append(indent).append("<div class=\"collapse\" ").append("id=").append(quotes(divId))
      .append(">").append(LS);

    //JSONArray
    level++;
    final JSONArray jarr = dropdn.getJSONArray("array");
    final Iterator<Object> itr = jarr.iterator();
    while (itr.hasNext()) {
      final JSONObject jo = (JSONObject) itr.next();
      final String clazz = jo.getString("class");
      if (clazz.equals("Dropdown")) { emitDropdown(jo, sb); }
      else { emitDoc(jo, sb); }
    }
    level--;

    sb.append(indent).append("</div>").append(LS);
  }


  void emitDoc(final JSONObject doc, final StringBuilder sb) {
    final String dir = doc.getString("dir");
    final String file = doc.getString("file");
    final String desc = doc.getString("desc");
    final boolean pdf = doc.optBoolean("pdf");
    final String indent = indent(level);
    sb.append(indent).append("<li><a href=\"");
    if (dir.equals("ROOT")) { sb.append("/"); }
    else {
      final String baseDir = pdf ? "{{site.docs_pdf_dir}}/" : "{{site.docs_dir}}/";
      sb.append(baseDir);
      if (!dir.isEmpty()) {
        sb.append(dir + "/");
      }
    }
    sb.append(file);
    final String sfx = pdf ? ".pdf" : ".html";
    sb.append(sfx + "\">");
    sb.append(desc);
    sb.append("</a></li>").append(LS);
  }


  public static String quotes(final String s) {
    return '"' + s + '"';
  }

  /**
   * @param level indention level
   * @return the indention spaces
   */
  public static String indent(final int level) {
    assert level >= 0;
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < level; i++) {
      sb.append("  ");
    }
    return sb.toString();
  }

  static void println(final String s) { System.out.println(s); }

}
