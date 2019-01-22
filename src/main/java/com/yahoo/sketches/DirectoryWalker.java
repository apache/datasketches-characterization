/*
 * Copyright 2015, Yahoo! Inc.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Recursive directory search.
 *
 * @author Lee Rhodes
 */
public class DirectoryWalker {
  protected static final String FS = System.getProperty("file.separator"); // "/"

  /**
   * A Node is a directory in a tree of directories.
   */
  private static class Node {
    private String nodePath__ = null;
    // List of sub-directory Nodes found by this Node only
    private ArrayList<Node> nodeDirList__ = null;
    private boolean recurseFlag__ = false;
    private Pattern filePattern__ = null;
    private ArrayList<String> nodeFileList__ = null; // cumulative file list

    /**
     * Construct a new directory Node.
     *
     * @param nodePath
     *          the path to this directory including the file separator "/"
     * @param filePattern
     *          the Pattern used to select files to be included in the fileList
     * @param recurseFlag
     *          if true, the sub-directories in this directory will be searched
     *          as well.
     * @param fileList
     *          the cumulative file list that is added to by each node searched.
     */
    Node(final String nodePath, final Pattern filePattern, final boolean recurseFlag,
        final ArrayList<String> fileList) {
      nodePath__ = nodePath;
      filePattern__ = filePattern;
      recurseFlag__ = recurseFlag;
      nodeFileList__ = fileList;
    }

    void buildLists() {
      File file = new File(nodePath__);
      final String[] strFileDirArr = file.list(); // get array of file/dir names in my directory
      if (strFileDirArr == null) {
        throw new IllegalArgumentException("File is not a valid dir.");
      }
      final int numFileDirs = strFileDirArr.length;
      for (int i = 0; i < numFileDirs; i++) { // scan all file/dirs at this node
        final String fileName = nodePath__ + strFileDirArr[i];
        file = new File(fileName);
        if (file.isDirectory()) {
          if (recurseFlag__) {
            if (nodeDirList__ == null) {
              nodeDirList__ = new ArrayList<>();
            }
            final Node node = new Node(fileName + FS, filePattern__, recurseFlag__, nodeFileList__);
            nodeDirList__.add(node);
          }
        } else { // it is a file
          if (filePattern__ != null) {
            if (filePattern__.matcher(fileName).matches()) {
              nodeFileList__.add(fileName); // add it if it matches
            }
          }
          else {
            nodeFileList__.add(fileName); // just add it
          }
        }
      }
    }
  } // End of class Node

  // Recursive routine
  private static void buildDirTree(final Node current, final boolean recursive) {
    current.buildLists(); // build the list for my node
    final ArrayList<Node> al = current.nodeDirList__;
    if ((al == null) || al.isEmpty() || !recursive) {
      return; // return if leaf node
    }
    final int numDirs = al.size(); // otherwise, go deeper
    for (int i = 0; i < numDirs; i++) {
      buildDirTree(al.get(i), recursive);
    }
  }

  /**
   * Creates a new List&lt;String&gt; of fileNames starting with the root directory
   * path.
   *
   * @param rootPath
   *          absolute or relative path and must end with a file separator.
   * @param regExSelector
   *          A RegEx matching pattern for putting a filename into the list. It
   *          may be null.
   * @param recursive
   *          If true, examine all subdirectories
   * @return an ArrayListFile of the list of paths + fileNames.
   */
  public static List<String> appendFileList(final String rootPath, final String regExSelector,
      final boolean recursive) {
    Pattern filePattern = null;
    if ((regExSelector != null) && (regExSelector.length() > 0)) {
      filePattern = Pattern.compile(regExSelector);
    }
    final ArrayList<String> fileList = new ArrayList<>();
    final Node root = new Node(rootPath, filePattern, recursive, fileList);
    buildDirTree(root, recursive);
    return fileList;
  }

  /**
   * blah
   * @param args blah
   */
  public static void main(final String[] args) {
    final String rootPath = "/Users/lrhodes/dev/git/DataSketches.github.io/_site/docs/";
    final String regExSelector = ".+[.]html";
    final boolean recursive = true;

    final List<String> fileList = appendFileList(rootPath, regExSelector, recursive);
    final int size = fileList.size();
    for (int i = 0; i < size; i++) {
      System.out.println(fileList.get(i));
    }
  }
}
