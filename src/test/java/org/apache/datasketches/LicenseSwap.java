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

import java.util.List;

import org.testng.annotations.Test;

import org.apache.datasketches.DirectoryWalker;

import static java.lang.Math.*;

import java.io.File;

/**
 * @author Lee Rhodes
 */
@SuppressWarnings("unused")
public class LicenseSwap {
  private static final String LS = System.getProperty("line.separator");
  private static enum CommentType { JAVA, SCRIPT }
  private static String before;
  private static String esc;
  private static String after;


  //CONFIGURE:
  private static String mySystemPath = "/Users/lrhodes/dev/git/";
  private static String myRepoPath = "Apache/datasketches-vector/"; //no incubator here
  private static String folderPath = "src/";  //usually: src or docs
  private static String rootPath = mySystemPath + myRepoPath + folderPath;
  private static String fileSelector = ".+[.]java";
  private static boolean detail = true;
  private static CommentType comment = CommentType.JAVA;

  static {
    switch (comment) {
      case JAVA: {
        before = "/*" + LS;
        esc    = " *";
        after  = " */" + LS;
        break;
      }
      case SCRIPT: {
        before = "";
        esc    = "#";
        after  = LS;
        break;
      }
    }
  }

  /**
   * <p>PUT TARGET IN A BRANCH FIRST!</p>
   *
   * <p>Run this test to perform the selected operation (below).</p>
   *
   * <p>AFTERWARDS, UPDATE POM!</p>
   */
  @Test
  public static void treeWalk() {
    final boolean recursive = true;
    final List<String> fileList = DirectoryWalker.appendFileList(rootPath, fileSelector, recursive);
    final int numFiles = fileList.size();
    println("Files: " + numFiles + "\n");
    for (int i = 0; i < numFiles; i++) {
      String pathFile = fileList.get(i);
      //SELECT!
      //insertLicense(pathFile);
      //replaceStrings(pathFile);
      replaceLicense(pathFile);
      replacePackage(pathFile);
    }
    println("DONE!");
  }

  private static void insertLicense(String pathFile) {
    String fileStr = Files.fileToString(pathFile);
    Files.stringToFile(asfHeader + fileStr, pathFile);
    if (detail) { println(pathFile); }
  }

  private static void replaceLicense(String pathFile) {
    String fileStr = Files.fileToString(pathFile);
    int i1 = fileStr.indexOf("/**"); //detects package-info comments before "package" statememt
    int i2 = fileStr.indexOf("package");
    if (i2 > 0) { i2 = fileStr.indexOf("\npackage") + 1; }
    if (i2 < 0) {
      throw new IllegalArgumentException("No package in " + pathFile);
    }
    int i3 = (i1 < 0) ? i2 : min(i1, i2);

    if (detail) {
      String filename = pathFile.substring(pathFile.lastIndexOf("/") + 1, pathFile.length());
      println(filename + ":\n" + fileStr.substring(0, i3));
    }

    String newFileStr = asfHeader + LS + fileStr.substring(i3);
    Files.stringToFile(newFileStr, pathFile);
  }

  private static void replaceStrings(String pathFile) {
    String fileStr = Files.fileToString(pathFile); //entire contents of file
    //first change the references to memory
    String fileStr2 = fileStr.replace("com.yahoo.memory", "org.apache.datasketches.memory");
    String fileStr3 = fileStr2.replace("com/yahoo/memory", "org/apache/datasketches/memory");
    //then change the the rest of the strings
    String fileStr4 = fileStr3.replace("com.yahoo.sketches", "org.apache.datasketches");
    String fileStr5 = fileStr4.replace("com/yahoo/sketches", "org/apache/datasketches");
    Files.stringToFile(fileStr5, pathFile);
  }

  private static void replacePackage(String pathFile) {
    String fileStr = Files.fileToString(pathFile); //entire contents of file
    //first change the references to memory
    String fileStr2 = fileStr.replace("com.yahoo.memory", "org.apache.datasketches.memory");
    //then change the the rest of the hierarchy
    String fileStr3 = fileStr2.replace("com.yahoo.sketches", "org.apache.datasketches");
    //now restore in new directory
    int idx = pathFile.lastIndexOf("/") + 1;
    String path = pathFile.substring(0, idx); //with the slash
    String file = pathFile.substring(idx);
    String path2;
    if (path.contains("com/yahoo/memory")) {
      path2 = path.replace("com/yahoo/memory", "org/apache/datasketches/memory");
      if (detail) {
        println(path + " --> " + path2);
      }
    } else if (path.contains("com/yahoo/sketches")) {
      path2 = path.replace("com/yahoo/sketches", "org/apache/datasketches");
      if (detail) {
        println(path + " --> " + path2);
      }
    } else {
      throw new IllegalArgumentException(path);
    }
    File newDir = new File(path2);
    newDir.mkdirs();
    Files.stringToFile(fileStr3, path2 + file);
    File oldFile = new File(pathFile);
    oldFile.delete();
  }

  private static String asfHeader =
       before
     + esc + " Licensed to the Apache Software Foundation (ASF) under one" + LS
     + esc + " or more contributor license agreements.  See the NOTICE file" + LS
     + esc + " distributed with this work for additional information" + LS
     + esc + " regarding copyright ownership.  The ASF licenses this file" + LS
     + esc + " to you under the Apache License, Version 2.0 (the" + LS
     + esc + " \"License\"); you may not use this file except in compliance" + LS
     + esc + " with the License.  You may obtain a copy of the License at" + LS
     + esc + LS
     + esc + " http://www.apache.org/licenses/LICENSE-2.0" + LS
     + esc + LS
     + esc + " Unless required by applicable law or agreed to in writing," + LS
     + esc + " software distributed under the License is distributed on an" + LS
     + esc + " \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY" + LS
     + esc + " KIND, either express or implied.  See the License for the" + LS
     + esc + " specific language governing permissions and limitations" + LS
     + esc + " under the License." + LS
     + after;

  static void println(String s) { System.out.println(s); }

  /**********************************/

  @Test //test individual file first
  public void checkSingleFile() {
    String path = rootPath + "main/java/com/yahoo/sketches/theta/AnotB.java";
    replacePackage(path);
  }

}
