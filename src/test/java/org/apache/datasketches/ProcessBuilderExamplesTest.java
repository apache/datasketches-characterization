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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.testng.annotations.Test;

// https://www.tutorialspoint.com/java/lang/processbuilder_command_list.htm
// https://www.baeldung.com/java-lang-processbuilder-api
// https://github.com/eugenp/tutorials/blob/master/core-java-modules/core-java-os/src/test/java/com/baeldung/processbuilder/ProcessBuilderUnitTest.java

@SuppressWarnings({"resource", "unused"})
public class ProcessBuilderExamplesTest {

  private static final String c14nClasses =
      "/Users/lrhodes/dev/git/Apache/datasketches-characterization/target/classes:";

  private static final String coreJavaJars =
      "/Users/lrhodes/.m2/repository/org/apache/datasketches/datasketches-java/1.3.0-incubating/datasketches-java-1.3.0-incubating.jar:"
    + "/Users/lrhodes/.m2/repository/org/apache/datasketches/datasketches-memory/1.2.0-incubating/datasketches-memory-1.2.0-incubating.jar:"
    + "/Users/lrhodes/.m2/repository/org/slf4j/slf4j-api/1.7.27/slf4j-api-1.7.27.jar:"
    + "/Users/lrhodes/.m2/repository/org/slf4j/slf4j-simple/1.7.27/slf4j-simple-1.7.27.jar:";

  private static final String testNGJars =
      "/Users/lrhodes/.m2/repository/org/testng/testng/6.14.3/testng-6.14.3.jar:"
    + "/Users/lrhodes/.m2/repository/com/beust/jcommander/1.72/jcommander-1.72.jar:"
    + "/Users/lrhodes/.m2/repository/org/apache-extras/beanshell/bsh/2.0b6/bsh-2.0b6.jar:";

  @Test
  public void runJavaThetaTest() throws IOException, InterruptedException {

    final String clsPath = c14nClasses + coreJavaJars; // + testNGJars;

    List<String> list = new ArrayList<>();
    list.add("java");
    list.add("-Dfile.encoding=UTF-8");
    list.add("-server");
    list.add("-d64");
    list.add("-ea");
    list.add("-classpath");
    list.add(clsPath);
    list.add("org.apache.datasketches.RunJob");

    ProcessBuilder processBuilder = new ProcessBuilder(list);
    processBuilder.redirectErrorStream(true);

    Process process = processBuilder.start();

    List<String> results = readOutput(process.getInputStream());
    assertThat("Results should not be empty", results, is(not(empty())) );

    println("");
    results.forEach((item) -> println(item));
    println("");

    int exitCode = process.waitFor();
    assertEquals("No errors should be detected", 0, exitCode);
  }



  @Test
  public void givenProcessBuilder_whenInvokeStart_thenSuccess()
        throws IOException, InterruptedException {
      ProcessBuilder processBuilder = new ProcessBuilder("java", "-version");
      processBuilder.redirectErrorStream(true);

      Process process = processBuilder.start();

      List<String> results = readOutput(process.getInputStream());
      println("");
      results.forEach((item) -> println(item));
      println("");
      assertThat("Results should not be empty", results, is(not(empty())) );
      assertThat("Results should contain openjdk version: ", results,
          hasItem(containsString("openjdk version")));

      int exitCode = process.waitFor();
      assertEquals("No errors should be detected", 0, exitCode);
  }

  @Test
  public void givenProcessBuilder_whenModifyEnvironment_thenSuccess()
        throws IOException, InterruptedException {
      ProcessBuilder processBuilder = new ProcessBuilder();
      Map<String, String> environment = processBuilder.environment();

      environment.put("GREETING", "Hola Mundo");
      //environment.forEach((key, value) -> System.out.println("KEY  : " + key + "\nVALUE: " + value + "\n"));

      List<String> command = getGreetingCommand();
      processBuilder.command(command);
      Process process = processBuilder.start();

      List<String> results = readOutput(process.getInputStream());
      //results.forEach((item) -> println(item));
      assertThat("Results should not be empty", results, is(not(empty())));
      assertThat("Results should contain a greeting ", results,
          hasItem(containsString("Hola Mundo")));

      int exitCode = process.waitFor();
      assertEquals("No errors should be detected", 0, exitCode);
  }

  @Test
  public void givenProcessBuilder_whenModifyWorkingDir_thenSuccess()
        throws IOException, InterruptedException {
      List<String> command = getDirectoryListingCommand();
      ProcessBuilder processBuilder = new ProcessBuilder(command);

      File dir = processBuilder.directory();
      println(dir == null ? System.getProperty("user.dir") : dir.getName());
      processBuilder.directory(new File("src"));
      dir = processBuilder.directory();
      println(dir == null ? System.getProperty("user.dir") : dir.getName());

      Process process = processBuilder.start();

      List<String> results = readOutput(process.getInputStream());
      results.forEach((item) -> println(item));

      assertThat("Results should not be empty", results, is(not(empty())));
      assertThat("Results should contain directory listing: ", results,
          hasItems(containsString("main"), containsString("test")));

      int exitCode = process.waitFor();
      assertEquals("No errors should be detected", 0, exitCode);
  }

  private static File makeFile(final String path, final String fileName)
      throws IOException {
    File pth = new File(path);
    if (pth.exists()) { pth.delete(); }
    pth.mkdir();
    pth.deleteOnExit();
    File log = new File(path + fileName);
    log.createNewFile();
    log.deleteOnExit();
    return log;
  }

  private static final String pathStr =
      "/Users/lrhodes/dev/git/Apache/datasketches-characterization/tempFolder/";

  @Test
  public void givenProcessBuilder_whenRedirectStandardOutput_thenSuccessWriting()
        throws IOException, InterruptedException {
      ProcessBuilder processBuilder = new ProcessBuilder("java", "-version");
      processBuilder.redirectErrorStream(true);

      File log = makeFile(pathStr, "java-version.log");
      processBuilder.redirectOutput(log);

      Process process = processBuilder.start();

      assertEquals("If redirected, should be -1 ", -1, process.getInputStream()
          .read());
      int exitCode = process.waitFor();
      assertEquals("No errors should be detected", 0, exitCode);

      List<String> lines = Files.lines(log.toPath())
          .collect(Collectors.toList());
      //lines.forEach((item) -> println(item)); //what is also in log

      assertThat("Results should not be empty", lines, is(not(empty())));
      assertThat("Results should contain openjdk version: ", lines,
          hasItem(containsString("openjdk version")));
  }

  @Test
  public void givenProcessBuilder_whenRedirectStandardOutput_thenSuccessAppending()
        throws IOException, InterruptedException {
      ProcessBuilder processBuilder = new ProcessBuilder("java", "-version");

      File log = makeFile(pathStr, "java-version-append.log");

      processBuilder.redirectErrorStream(true);
      processBuilder.redirectOutput(Redirect.appendTo(log));

      Process process = processBuilder.start();

      assertEquals("If redirected output, should be -1 ", -1, process.getInputStream()
          .read());

      int exitCode = process.waitFor();
      assertEquals("No errors should be detected", 0, exitCode);

      List<String> lines = Files.lines(log.toPath())
          .collect(Collectors.toList());

      assertThat("Results should not be empty", lines, is(not(empty())));
      assertThat("Results should contain openjdk version: ", lines,
          hasItem(containsString("openjdk version")));
  }

//  @Test  //ProcessBuilder.startPipeline() only available in Java 9+
//  public void givenProcessBuilder_whenStartingPipeline_thenSuccess()
//        throws IOException, InterruptedException {
//      if (!isWindows()) {
//          List<ProcessBuilder> builders = Arrays.asList(
//              new ProcessBuilder("find", "src", "-name", "*.java", "-type", "f"),
//              new ProcessBuilder("wc", "-l"));
//
//          List<Process> processes = ProcessBuilder.startPipeline(builders); //requires JDK9
//          Process last = processes.get(processes.size() - 1);
//
//          List<String> output = readOutput(last.getInputStream());
//          assertThat("Results should not be empty", output, is(not(empty())));
//      }
//  }

  @Test
  public void givenProcessBuilder_whenInheritIO_thenSuccess()
        throws IOException, InterruptedException {
      List<String> command = getEchoCommand();
      ProcessBuilder processBuilder = new ProcessBuilder(command);

      processBuilder.inheritIO();
      Process process = processBuilder.start();

      int exitCode = process.waitFor();
      assertEquals("No errors should be detected", 0, exitCode);
  }

  @SuppressWarnings("static-method")
  private List<String> readOutput(InputStream inputStream)
        throws IOException {
      try (BufferedReader output = new BufferedReader(new InputStreamReader(inputStream))) {
          return output.lines()
              .collect(Collectors.toList());
      }
  }

  private List<String> getDirectoryListingCommand() {
      return isWindows() ? Arrays.asList("cmd.exe", "/c", "dir")
          : Arrays.asList("/bin/sh", "-c", "ls");
  }

  private List<String> getGreetingCommand() {
      return isWindows() ? Arrays.asList("cmd.exe", "/c", "echo %GREETING%")
          : Arrays.asList("/bin/bash", "-c", "echo $GREETING");
  }

  private List<String> getEchoCommand() {
      return isWindows() ? Arrays.asList("cmd.exe", "/c", "echo hello")
          : Arrays.asList("/bin/sh", "-c", "echo hello");
  }

  @SuppressWarnings("static-method")
  private boolean isWindows() {
      return System.getProperty("os.name")
          .toLowerCase()
          .startsWith("windows");
  }

  static void println(Object o) { System.out.println(o.toString()); }
}