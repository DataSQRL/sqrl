package com.datasqrl.text;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Returns an array of substrings by splitting the input string based on the given delimiter.
 * If the delimiter is not found in the string, the original string is returned as the only element
 * in the array. If the delimiter is empty, every character in the string is split. If the string or
 * delimiter is null, a null value is returned. If the delimiter is found at the beginning or end of
 * the string, or there are contiguous delimiters, then an empty string is added to the array.
 */
public class Split extends ScalarFunction {

  public String[] eval(String text, String delimiter) {
    if (text == null || delimiter == null) {
      return null;
    }

    if (delimiter.isEmpty()) {
      return text.split("");
    }

    return text.split(delimiter, -1);
  }

}