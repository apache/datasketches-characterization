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

import static org.apache.datasketches.Files.isFileValid;
import static org.apache.datasketches.Files.openPrintWriter;
import static org.apache.datasketches.Util.getResourcePath;
import static org.apache.datasketches.Util.milliSecToString;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

/**
 * This class parses an input string job file, which contains properties for a specific
 * JobProfile, and then loads and runs the JobProfile.
 *
 * @author Lee Rhodes
 */
public class Job {
  static final String LS = System.getProperty("line.separator");
  private Properties prop;
  //Output to Files
  private PrintWriter pw = null;
  private PrintWriter pwData = null;
  //Date-Time
  private SimpleDateFormat fileSimpleDateFmt;  //used in the filename
  private SimpleDateFormat readableSimpleDateFmt; //for human readability
  private GregorianCalendar gCal;
  private long startTime_mS;
  private JobProfile profile;
  private final String profileName;

  /**
   * Build properties and run the Job Profile
   *
   * @param jobConfigureFileName the name of the text configuration file containing the properties for the
   * JobProfile to be run.
   */
  public Job(final String jobConfigureFileName) {
    final String jobConfStr;
    if (isFileValid(jobConfigureFileName)) { //assumes fully qualified
      jobConfStr = Files.fileToString(jobConfigureFileName); //includes line feeds
    } else {
      final String path = getResourcePath(jobConfigureFileName); //try resources
      jobConfStr = Files.fileToString(path); //includes line feeds
    }
    prop = parseJobProperties(jobConfStr);

    profile = createJobProfile();
    profileName = profile.getClass().getSimpleName();

    setDateFormats();
    configurePrintWriters();
    if (pw == null || pwData == null) {
      throw new IllegalStateException("Could not configure PrintWriters.");
    }

    println("START JOB " + profileName );
    flush(); //flush print buffer

    /***RUN THE PROFILE ****************/
    startTime_mS = System.currentTimeMillis();

    profile.start(this);

    final long testTime_mS = System.currentTimeMillis() - startTime_mS;
    /***********************************/
    println("PROPERTIES:");
    println(prop.extractKvPairs(LS));
    println("Total Job Time        : " + milliSecToString(testTime_mS));
    println("END JOB " + profileName +  LS + LS);
    flush();
    pw.close();
    pwData.close();
  }

  /**
   * Gets a human-readable date string given the time in milliseconds.
   * @param timeMillisec the given time generated from System.currentTimeMillis().
   * @return the date string
   */
  public String getReadableDateString(final long timeMillisec) {
    gCal.setTimeInMillis(timeMillisec);
    return readableSimpleDateFmt.format(gCal.getTime());
  }

  /**
   * Gets the start time in milliseconds
   * @return the start time in milliseconds
   */
  public long getStartTime() {
    return startTime_mS;
  }

  /**
   * Gets the Properties class.
   * @return the Properties class.
   */
  public Properties getProperties() {
    return prop;
  }

  /**
   * Outputs a string to the configured PrintWriter and stdOut.
   * @param obj The obj.toString() to print
   */
  public final void print(final Object obj) {
    System.out.print(obj.toString());
    pw.print(obj.toString());
  }

  /**
   * Outputs a string to the configured PrintWriter for data and stdOut.
   * @param obj The obj.toString() to print
   */
  public final void printData(final Object obj) {
    System.out.print(obj.toString());
    pwData.print(obj.toString());
  }

  /**
   * Outputs a line to the configured PrintWriter and stdOut.
   * @param obj The obj.toString() to print
   */
  public final void println(final Object obj) {
    System.out.println(obj.toString());
    pw.println(obj.toString());
  }

  /**
   * Outputs a line to the configured PrintWriter for data and stdOut.
   * @param obj The obj.toString() to print
   */
  public final void printlnData(final Object obj) {
    System.out.println(obj.toString());
    pwData.println(obj.toString());
  }

  /**
   * Outputs a formatted set of arguments to PrintWriter and stdOut.
   * @param format the format specification
   * @param args the list of objects
   */
  public final void printf(final String format, final Object ...args) {
    System.out.printf(format, args);
    pw.printf(format, args);
  }

  /**
   * Outputs a formatted set of arguments to PrintWriter for data and stdOut.
   * @param format the format specification
   * @param args the list of objects
   */
  public final void printfData(final String format, final Object ...args) {
    System.out.printf(format, args);
    pwData.printf(format, args);
  }

  /**
   * Flush any buffered output to the configured PrintWriters.
   */
  public final void flush() {
    pw.flush();
    pwData.flush();
  }

  public final PrintWriter getPrintWriter() {
    return pw;
  }

  public final PrintWriter getDataPrintWriter() {
    return pwData;
  }

  /**
   * Each job is assigned a new Properties class, which is simply a hash map of string
   * key value pairs. The pairs are separated by System.getProperty("line.separator").
   * The key is separated from the value by "=". Comments start with "#" and continue to the
   * end of the line.
   * @param jobStr the all the properties as a single string
   */
  private static final Properties parseJobProperties(final String jobStr) {
    final Properties prop = new Properties();
    final String[] lines = jobStr.split(LS);
    for (int i = 0; i < lines.length; i++) {
      final String line = lines[i].trim();
      final int commentIdx = line.indexOf('#');
      final String tmp;
      if (commentIdx >= 0) { //comment
        tmp = line.substring(0, commentIdx).trim();
      } else {
        tmp = line;
      }
      if (tmp.length() < 3) { continue; }
      final String[] kv = tmp.split("=", 2);
      if (kv.length < 2) {
        throw new IllegalArgumentException("Missing valid key-value separator: " + tmp);
      }
      prop.put(kv[0].trim(), kv[1].trim());
    }
    return prop;
  }

  /**
   * Set two date formats into the Properties file.
   * The default date format for the output file name is "yyyyMMdd'_'HHmmssz".
   * This can be overridden using the key "FileNameDateFormat".
   *
   * <p>The default date format for the reports is "yyyy/MM/dd HH:mm:ss z".
   * This can be overridden using the key "ReadableDateFormat".
   *
   * <p>The default time-zone is GMT, with an TimeZoneOffset of zero.
   * This can be overridden using the key "TimeZone" for the 3 letter abreviation of the time
   * zone name, and the key "TimeZoneOffset" to specify the offset in milliseconds.
   */
  private final void setDateFormats() {
    String fileSdfStr = prop.get("FileNameDateFormat");
    if (fileSdfStr == null) {
      fileSdfStr = "yyyyMMdd'_'HHmmssz";
      prop.put("FileNameDateFormat", "yyyyMMdd'_'HHmmssz");
    }
    fileSimpleDateFmt = new SimpleDateFormat(fileSdfStr);

    String readableSdfStr = prop.get("ReadableDateFormat");
    if (readableSdfStr == null) {
      readableSdfStr = "yyyy/MM/dd HH:mm:ss z";
      prop.put("ReadableDateFormat", readableSdfStr);
    }
    readableSimpleDateFmt = new SimpleDateFormat(readableSdfStr);

    String timeZoneStr = prop.get("TimeZone");
    if (timeZoneStr == null) {
      timeZoneStr = "GMT";
      prop.put("TimeZone", timeZoneStr);
    }
    String tzOffsetStr = prop.get("TimeZoneOffset");
    if (tzOffsetStr == null) {
      tzOffsetStr = "0";
      prop.put("TimeZoneOffset", tzOffsetStr);
    }
    final int timeZoneOffset = Integer.parseInt(tzOffsetStr);
    final SimpleTimeZone stz = new SimpleTimeZone(timeZoneOffset, timeZoneStr);
    fileSimpleDateFmt.setTimeZone(stz);
    readableSimpleDateFmt.setTimeZone(stz);
    gCal = new GregorianCalendar(stz);
    gCal.setFirstDayOfWeek(java.util.Calendar.SUNDAY); //Sun = 1, Sat = 7
  }

  private final JobProfile createJobProfile() {
    final String profileStr = prop.mustGet("JobProfile");
    final JobProfile profile;
    try {
      final Class<?> clazz = Class.forName(profileStr);
      profile = (JobProfile) clazz.getDeclaredConstructor().newInstance();
    } catch (final Exception e) {
      throw new RuntimeException("Cannot instantiate " + profileStr + "\n" + e);
    }
    return profile;
  }

  /**
   * Called from constructor to configure the Print Writer
   */
  private final void configurePrintWriters() {
    //create file name
    gCal.setTimeInMillis(System.currentTimeMillis());
    final String nowStr = fileSimpleDateFmt.format(gCal.getTime());

    final String outputFileName = profileName + nowStr + ".txt";
    final String outputFileNameData = profileName + nowStr + ".tsv";
    prop.put("OutputFileName", outputFileName);
    prop.put("OutputFileNameData", outputFileNameData);
    pw = openPrintWriter(outputFileName);
    pwData = openPrintWriter(outputFileNameData);
  }

  /**
   * The JVM may call this method to close the PrintWriter resources.
   */
  /* DEPRECATED in Object since java 9
  @Override
  protected void finalize() throws Throwable {
    try {
      if (pw != null) {
        pw.close();
      }
      if (pwData != null) {
        pwData.close();
      }
    } finally {
      super.finalize();
    }
  }
   */

  /**
   * Run multiple jobs from the command line
   * @param args the configuration file names to be run
   */
  @SuppressWarnings("unused")
  public static void main(final String[] args) {
    for (int j = 0; j < args.length; j++) {
      new Job(args[j]);
    }
  }

}
