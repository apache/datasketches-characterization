/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization;

import static com.yahoo.sketches.Files.isFileValid;
import static com.yahoo.sketches.Files.openPrintWriter;
import static com.yahoo.sketches.Util.milliSecToString;

import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;

import com.yahoo.sketches.Files;

/**
 * This class parses an input string job file, which contains properties for a specific
 * JobProfile, and then loads and runs the JobProfile.
 *
 * @author Lee Rhodes
 */
public class Job {
  private static final String LS = System.getProperty("line.separator");
  private Properties prop;
  //Output to File
  private PrintWriter pw = null;
  //Date-Time
  private SimpleDateFormat fileSimpleDateFmt;  //used in the filename
  private SimpleDateFormat readableSimpleDateFmt; //for human readability
  private GregorianCalendar gc;
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
    if (!isFileValid(jobConfigureFileName)) {
      throw new IllegalArgumentException("File not valid. " + jobConfigureFileName);
    }
    final String jobConfStr = Files.fileToString(jobConfigureFileName); //includes line feeds
    prop = parseJobProperties(jobConfStr);

    profile = createJobProfile();
    profileName = profile.getClass().getSimpleName();

    setDateFormats();
    pw = configurePrintWriter();
    if (pw == null) {
      throw new IllegalStateException("Could not configure PrintWriter.");
    }

    println("START JOB " + profileName );
    println(prop.extractKvPairs(LS));
    flush(); //flush print buffer

    /***RUN THE PROFILE ****************/
    startTime_mS = System.currentTimeMillis();

    profile.start(this);

    final long testTime_mS = System.currentTimeMillis() - startTime_mS;
    /*******************/

    println("Total Job Time        : " + milliSecToString(testTime_mS));
    println("END JOB " + profileName +  LS + LS);
    flush();
    pw.close();

  }

  /**
   * Gets a human-readable date string given the time in milliseconds.
   * @param timeMillisec the given time generated from System.currentTimeMillis().
   * @return the date string
   */
  public String getReadableDateString(final long timeMillisec) {
    gc.setTimeInMillis(timeMillisec);
    return readableSimpleDateFmt.format(gc.getTime());
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
   * Outputs a line to the configured PrintWriter and stdOut.
   * @param s The String to print
   */
  public final void println(final String s) {
    System.out.println(s);
    pw.println(s);
  }

  /**
   * Flush any buffered output to the configured PrintWriter.
   */
  public final void flush() {
    pw.flush();
  }

  public final PrintWriter getPrintWriter() {
    return pw;
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
   * @param prop Properties
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
    gc = new GregorianCalendar(stz);
    gc.setFirstDayOfWeek(java.util.Calendar.SUNDAY); //Sun = 1, Sat = 7
  }

  private final JobProfile createJobProfile() {
    final String profileStr = prop.mustGet("JobProfile");
    final JobProfile profile;
    try {
      profile = (JobProfile) Class.forName(profileStr).newInstance();
    } catch (final Exception e) {
      throw new RuntimeException("Cannot instantiate " + profileStr + "\n" + e);
    }
    return profile;
  }

  /**
   * Called from constructor to configure the Print Writer
   */
  private final PrintWriter configurePrintWriter() {
    //create file name
    gc.setTimeInMillis(System.currentTimeMillis());
    final String nowStr = fileSimpleDateFmt.format(gc.getTime());

    final String outputFileName = profileName + nowStr + ".txt";
    prop.put("OutputFileName", outputFileName);
    return openPrintWriter(outputFileName);
  }

  /**
   * The JVM may call this method to close the PrintWriter resource.
   */
  @Override
  protected void finalize() throws Throwable {
    try {
      if (pw != null) {
        pw.close(); // close open files
      }
    } finally {
      super.finalize();
    }
  }

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
