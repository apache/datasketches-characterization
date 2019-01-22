/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches;

import java.io.File;

import org.testng.annotations.Test;

import com.yahoo.sketches.ZipFiles;
import com.yahoo.sketches.UnzipFiles;

/**
 * @author Lee Rhodes
 */
public class ZipFilesTest {
  final File srcDir = new File("/Users/lrhodes/dev/git/characterization/src/main/resources");
  final String dstZipDirFile = "/Users/lrhodes/dev/git/characterization/local/test.zip";
  final String srcZipFile = dstZipDirFile;
  final String dstDir = "/Users/lrhodes/dev/git/characterization/local/testUnzipedDir";

  @Test
  public void checkZipping() {
    ZipFiles.zipDirectory(srcDir, dstZipDirFile);
    UnzipFiles.unzip(srcZipFile, dstDir);
  }
}
