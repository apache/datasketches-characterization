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
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Unzip hierarchical directories or single files.
 *
 * @author Lee Rhodes
 */
public class UnzipFiles {

  /**
   * Unzip hierarchical directories or single files.
   * @param srcZipFile in the full path form "/Users/user/srcZipFile.zip"
   * @param destDir in the full path form "/Users/user/destDir".
   * If it exists it will be deleted first.
   */
  public static void unzip(final String srcZipFile, final String destDir) {
    final File dstDir = new File(destDir);
    if (dstDir.exists()) { dstDir.delete(); }
    dstDir.mkdirs(); //create the output directory
    final byte[] buffer = new byte[1024]; //buffer for read and write data to file

    try (
        FileInputStream fis = new FileInputStream(srcZipFile);
        ZipInputStream zis = new ZipInputStream(fis)) {
      ZipEntry ze = zis.getNextEntry();
      while ((ze != null) && !ze.getName().startsWith("__")) {
        final String fileName = ze.getName();
        final File newFile = new File(destDir + File.separator + fileName);

        //create directories for sub directories in zip
        new File(newFile.getParent()).mkdirs();
        try (FileOutputStream fos = new FileOutputStream(newFile)) {
          int len;
          while ((len = zis.read(buffer)) > 0) {
            fos.write(buffer, 0, len);
          }
          fos.close();
        }
        zis.closeEntry(); //close this ZipEntry
        ze = zis.getNextEntry();
      }
      zis.closeEntry(); //close last ZipEntry
    } catch (final IOException e) {
        e.printStackTrace();
    }
  }

}
