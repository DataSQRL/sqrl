/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Strings;

import lombok.SneakyThrows;

public class BaseFileUtil {

  private static final int DELIMITER_CHAR = 46;


  public static Pair<String, String> separateExtension(String fileName) {
    if (Strings.isNullOrEmpty(fileName)) {
      return null;
    }
    var offset = fileName.lastIndexOf(DELIMITER_CHAR);
    if (offset == -1) {
      return Pair.of(fileName, "");
    } else {
      return Pair.of(fileName.substring(0, offset).trim(), fileName.substring(offset + 1).trim());
    }
  }

  @SneakyThrows
  public static String readFile(URI uri) {
    URL url = uri.toURL();
    StringBuilder content = new StringBuilder();

    try (InputStream inputStream = url.openStream();
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {

      String line;
      while ((line = bufferedReader.readLine()) != null) {
        content.append(line).append(System.lineSeparator());
      }
    }

    return content.toString();
  }
  @SneakyThrows
  public static String readFile(Path path) {
    return Files.readString(path);
  }
}
