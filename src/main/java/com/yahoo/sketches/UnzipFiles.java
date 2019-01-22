/*
 * Copyright 2019, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

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
      while (ze != null) {
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
