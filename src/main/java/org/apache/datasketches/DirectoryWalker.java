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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

//import org.testng.annotations.Test;

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
    private String nodePath = null;
    // List of sub-directory Nodes found by this Node only
    private ArrayList<Node> nodeDirList = null;
    private boolean recurseFlag = false;
    private Pattern filePattern = null;
    private ArrayList<String> nodeFileList = null; // cumulative file list

    /**
     * Construct a new directory Node.
     *
     * @param nodePath the path to this directory including the file separator "/"
     * @param filePattern the Pattern used to select files to be included in the fileList
     * @param recurseFlag if true, the sub-directories in this directory will be searched as well.
     * @param fileList the cumulative file list that is added to by each node searched.
     */
    Node(final String nodePath, final Pattern filePattern, final boolean recurseFlag,
        final ArrayList<String> fileList) {
      this.nodePath = nodePath;
      this.filePattern = filePattern;
      this.recurseFlag = recurseFlag;
      nodeFileList = fileList;
    }

    void buildLists() {
      File file = new File(nodePath);
      final String[] strFileDirArr = file.list(); // get array of file/dir names in my directory
      if (strFileDirArr == null) {
        throw new IllegalArgumentException("File is not a valid dir.");
      }
      final int numFileDirs = strFileDirArr.length;
      for (int i = 0; i < numFileDirs; i++) { // scan all file/dirs at this node
        final String fileName = nodePath + strFileDirArr[i];
        file = new File(fileName);
        if (file.isDirectory()) {
          if (recurseFlag) {
            if (nodeDirList == null) {
              nodeDirList = new ArrayList<>();
            }
            final Node node = new Node(fileName + FS, filePattern, recurseFlag, nodeFileList);
            nodeDirList.add(node);
          }
        } else { // it is a file
          if (filePattern != null) {
            if (filePattern.matcher(fileName).matches()) {
              nodeFileList.add(fileName); // add it if it matches
            }
          }
          else {
            nodeFileList.add(fileName); // just add it
          }
        }
      }
    }
  } // End of class Node

  /**
   * Recursive routine that builds the fileList for each node.
   * @param curDirNode the current directory node
   * @param recursive if true, recurse.
   */
  private static void buildDirTree(final Node curDirNode, final boolean recursive) {
    curDirNode.buildLists(); // build the list for my current node
    final ArrayList<Node> dirList = curDirNode.nodeDirList;
    if ((dirList == null) || dirList.isEmpty() || !recursive) {
      return; // return if leaf node
    }
    final int numDirs = dirList.size(); // otherwise, go deeper
    for (int i = 0; i < numDirs; i++) {
      buildDirTree(dirList.get(i), recursive);
    }
  }

  /**
   * Creates a new List&lt;String&gt; of fileNames starting with the root directory path.
   *
   * @param rootPath absolute or relative path and must end with a file separator.
   * @param fileSelector A RegEx matching string for putting a filename into the list.
   * It may be null, which selects all files.
   * @param recursive If true, examine all subdirectories
   * @return an ArrayListFile of the list of paths + fileNames.
   */
  public static List<String> appendFileList(final String rootPath, final String fileSelector,
      final boolean recursive) {
    Pattern filePattern = null;
    if ((fileSelector != null) && (fileSelector.length() > 0)) {
      filePattern = Pattern.compile(fileSelector);
    }
    final ArrayList<String> fileList = new ArrayList<>();
    final Node root = new Node(rootPath, filePattern, recursive, fileList);
    buildDirTree(root, recursive);
    return fileList;
  }

  //@Test //example
  public static void printFiles() {
    final String rootPath = "/Users/lrhodes/dev/git/Apache/datasketches-memory/src/";
    final String fileSelector = ".+[.]java";
    final boolean recursive = true;

    final List<String> fileList = appendFileList(rootPath, fileSelector, recursive);
    final int size = fileList.size();

    for (int i = 0; i < size; i++) {
      println(fileList.get(i));
    }
    println("Files: " + size);
  }

  static void println(final String s) { System.out.println(s); }

}
