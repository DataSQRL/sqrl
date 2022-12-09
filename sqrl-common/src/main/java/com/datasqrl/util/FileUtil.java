/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;


import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Strings;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.Pair;

public class FileUtil {

  public static String getExtension(Path p) {
    return FilenameUtils.getExtension(p.getFileName().toString());
  }

  public static String removeExtension(Path p) {
    return FilenameUtils.removeExtension(p.getFileName().toString());
  }

  public static String readResource(String resourceName) throws IOException {
    URL url = Resources.getResource(resourceName);
    return Resources.toString(url, StandardCharsets.UTF_8);
  }

  public static boolean isEmptyDirectory(Path dir) throws IOException {
    if (!Files.isDirectory(dir)) return true;
    try (Stream<Path> entries = Files.list(dir)) {
      return !entries.findFirst().isPresent();
    }
  }

  public static <T> T executeFileRead(Path p, ExecuteFileRead<T> exec, ErrorCollector errors) {
    try {
      return exec.execute(p);
    } catch (IOException e) {
      errors.fatal("Could not read file or directory [%s]: [%s]", p, e);
      return null;
    }
  }

  private static final int DELIMITER_CHAR = 46;

  public static Pair<String, String> separateExtension(String fileName) {
    if (Strings.isNullOrEmpty(fileName)) {
      return null;
    }
    int offset = fileName.lastIndexOf(DELIMITER_CHAR);
    if (offset == -1) {
      return Pair.of(fileName, "");
    } else {
      return Pair.of(fileName.substring(0, offset).trim(), fileName.substring(offset + 1).trim());
    }
  }

  @FunctionalInterface
  public interface ExecuteFileRead<T> {

    T execute(Path p) throws IOException;

  }

}
