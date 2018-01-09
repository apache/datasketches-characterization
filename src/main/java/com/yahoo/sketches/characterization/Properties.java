/*
 * Copyright 2017, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

package com.yahoo.sketches.characterization;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

/**
 * @author Lee Rhodes
 */
public class Properties {
  private HashMap<String, String> map;

  public Properties() {
    map = new HashMap<>();
  }

  /**
   * Put a key-value pair into this map, replacing the value if the key already exists.
   * @param key the given key
   * @param value the given value
   * @return the previous value or null, if the key did not exist.
   */
  public String put(final String key, final String value) {
    return map.put(key, value);
  }

  /**
   * Get the value associated with the given key.
   * Throws exception if key is null or empty
   * @param key the given key
   * @return the value associated with the given key
   */
  public String mustGet(final String key) {
    final String v = map.get(key);
    if ((v == null) || (v.isEmpty())) {
      throw new IllegalArgumentException("Key: " + key + " not found or empty.");
    }
    return v;
  }

  /**
   * Get the value associated with the given key.
   * If the key does not exist, this returns null.
   *
   * @param key the given key
   * @return the value associated with the given key. It may be empty.
   */
  public String get(final String key) {
    return map.get(key);
  }

  /**
   * Merge the given properties into this one. Any duplicate keys will be replaced with the
   * latest value.
   * @param prop the given Properties.
   * @return this Properties
   */
  public Properties merge(final Properties prop) {
    final String kvPairs = prop.extractKvPairs();
    loadKvPairs(kvPairs);
    return this;
  }

  /**
   * Load the string containing key-value pairs into the map.
   * key-value pairs are split by the RegEx: "[,\t\n]".
   * Each key-value pair is split by the RegEx: "[=]".
   * Beginning and ending spaces are removed.
   * @param kvPairs the given string
   */
  public void loadKvPairs(final String kvPairs) {
    final String[] pairs = kvPairs.split("[,\t\n]");
    for (String pair : pairs) {
      final String[] kv = pair.split("=", 2);
      if (kv.length < 2) {
        throw new IllegalArgumentException("Missing valid key-value separator");
      }
      final String k = kv[0].trim();
      final String v = kv[1].trim();
      map.put(k, v);
    }
  }

  /**
   * Extract a sorted String representing all the KV pairs of this map.
   * Returns an empty string if the map is empty.
   * Keys are separated from values with "=". Key-value pairs are separate with ",".
   * If the map is not empty, the final character will be a comma ",".
   * @return a sorted String representing all the KV pairs of this map.
   */
  public String extractKvPairs() {
    return extractKvPairs(",");
  }

  /**
   * Extract a sorted String representing all the KV pairs of this map.
   * Returns an empty string if the map is empty.
   * Keys are separated from values with "=". Key-value pairs are separate with the pairSeparator.
   * If the map is not empty, the final character will be a pairSeparator.
   * @param pairSeparator the string to use to separate the key-value pairs.
   * @return a sorted String representing all the KV pairs of this map
   */
  public String extractKvPairs(final String pairSeparator) {
    final ArrayList<String> list = new ArrayList<>();
    map.forEach((key, value) -> {
      final String s = key + "=" + value + pairSeparator;
      list.add(s);
    });
    list.sort(Comparator.naturalOrder());
    final Iterator<String> itr = list.iterator();
    final StringBuilder sb = new StringBuilder();
    itr.forEachRemaining((s) -> {
      sb.append(s);
    });
    return sb.toString();
  }

  /**
   * Removes the trailing comma if there is one.
   * @param kvPairs a sequence of key-value pairs separated by pairSeparator.
   * @param pairSeparator the character used to separate the key-value pairs.
   * @return the same string but with the trailing comma removed if there was one.
   */
  public static String removeLastPairSeparator(final String kvPairs, final char pairSeparator) {
    final StringBuilder sb = new StringBuilder(kvPairs);
    final int len = sb.length();
    final Character last = sb.charAt(len - 1);
    if (last.equals(pairSeparator)) {
      sb.deleteCharAt(len - 1);
    }
    return sb.toString();
  }
}
