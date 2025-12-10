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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Zip hierarchical directories or single files.
 *
 * @author Lee Rhodes
 */
public class ZipFiles {

  /**
   * Zip hierarchical directories.
   * @param srcDir in the full path form "/Users/user/srcDir"
   * @param dstZipDirFile in the full path form "/Users/user/dstZipDirFile.zip"
   * If it exists, it will be deleted first.
   */
  public static void zipDirectory(final File srcDir, final String dstZipDirFile) {
    final File outFile = new File(dstZipDirFile);
    if (outFile.exists()) { outFile.delete(); }

    final List<String> filesListInDir = new ArrayList<>();
    try {
      populateFilesList(srcDir, filesListInDir);
      //now zip files one by one
      try (
          FileOutputStream fos = new FileOutputStream(dstZipDirFile);
          ZipOutputStream zos = new ZipOutputStream(fos)) {
        for (String filePath : filesListInDir) {
          System.out.println("Zipping " + filePath);
          //for ZipEntry we need to keep only relative file path, so use substring on absolute path
          final ZipEntry ze = new ZipEntry(filePath.substring(srcDir.getAbsolutePath().length() + 1,
              filePath.length()));
          zos.putNextEntry(ze);
          //read the file and write to ZipOutputStream
          try (FileInputStream fis = new FileInputStream(filePath)) {
            final byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) > 0) {
              zos.write(buffer, 0, len);
            }
            zos.closeEntry();
          }
        }
      }
    } catch (final IOException e) {
        e.printStackTrace();
    }
  }

  /**
   * This method populates all the files in a directory to a List recursively
   * @param srcDir the source directory
   * @throws IOException for IO errors
   */
  private static void populateFilesList(final File srcDir, final List<String> filesListInDir)
      throws IOException {
    final File[] files = srcDir.listFiles();
    for (File file : files) {
      if (file.isFile()) {
        filesListInDir.add(file.getAbsolutePath());
      } else { //if directory, recurse
        populateFilesList(file, filesListInDir);
      }
    }
  }

  /**
   * Zip a single file.
   * @param srcFile the source file
   * @param zipFileName the destination zip file. If it exists it will be deleted first.
   */
  public static void zipSingleFile(final File srcFile, final String zipFileName) {
    final File outFile = new File(zipFileName);
    if (outFile.exists()) { outFile.delete(); }

    try (
        FileInputStream fis = new FileInputStream(srcFile);
        FileOutputStream fos = new FileOutputStream(zipFileName);
        ZipOutputStream zos = new ZipOutputStream(fos)) {

      //add a new Zip Entry to the ZipOutputStream
      final ZipEntry ze = new ZipEntry(srcFile.getName());
      zos.putNextEntry(ze);

      //read the file and write to ZipOutputStream
      final byte[] buffer = new byte[1024];
      int len;
      while ((len = fis.read(buffer)) > 0) {
        zos.write(buffer, 0, len);
      }

      zos.closeEntry(); //Close the zip entry to write to zip file
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }

}
