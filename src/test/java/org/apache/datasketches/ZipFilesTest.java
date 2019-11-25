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

import org.apache.datasketches.ZipFiles;
import org.apache.datasketches.UnzipFiles;

/**
 * Quick zip and unzip test. Must modifiy the "localBaseDir" and "localDstDir" for your environment.
 *
 * @author Lee Rhodes
 */
public class ZipFilesTest {
  final String localBaseDir = "/Users/lrhodes/dev/git/Apache/datasketches-characterization";
  final String localDstDir = "/local";

  final File srcDir = new File(localBaseDir + "/src/main/resources");
  final String dstZipDirFile = localBaseDir + localDstDir + "/test.zip";
  final String srcZipFile = dstZipDirFile;
  final String dstDir = localBaseDir + localDstDir + "/testUnzipedDir";

  //@Test
  public void checkZipping() {
    ZipFiles.zipDirectory(srcDir, dstZipDirFile);
    UnzipFiles.unzip(srcZipFile, dstDir);
  }
}
