/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Base64;

import org.apache.datasketches.hll.HllSketch;
import org.testng.annotations.Test;

/**
 * @author Lee Rhodes
 */
public class DumpSketchSegment {
  final static Base64.Decoder decoder = Base64.getDecoder();
  final String fname = "/Users/lrhodes/dev/git/Apache/druid-0.18.0/Dump/hll_segment.txt";
  final String tgtDir = "/Users/lrhodes/dev/git/Apache/druid-0.18.0/Dump/Sketches/";
  final ProcessSketch processSketch = new ProcessSketch();

  @Test
  public void analyzeSegment() {
    final LineReader lineReader = new LineReader(fname);
    lineReader.read(0, processSketch);
  }

  class ProcessSketch implements ProcessLine {

    @Override
    public void process(final String line, final int lineNo) {
      final String[] splits = line.split(":");
      if (splits.length != 2) { throw new IllegalArgumentException("Splits != 2, Line#: " + lineNo); }
      final String s1 = splits[1];
      final String substr = s1.substring(1, s1.length() - 2);
      final byte[] byteArray = decoder.decode(substr);
      //writeByteArrayToFile(byteArray, tgtDir + "HllSketch" + lineNo);
      final HllSketch sk = HllSketch.heapify(byteArray);
      println("HllSketch " + lineNo);
      println(HllSketch.toString(byteArray));
      println(sk.toString(true, false, false, false));
      println("*****************");
    }

  }

  private static void writeByteArrayToFile(final byte[] arr, final String fullFileName) {
    Files.checkFileName(fullFileName); //checks for null, empty
    final File file = new File(fullFileName);
    if (file.exists() && file.isFile()) {
      file.delete();
    }
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(arr);
      fos.flush();
      fos.close();
    } catch (final IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  static void println(final Object obj) { System.out.println(obj.toString()); }
}
