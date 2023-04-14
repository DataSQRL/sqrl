/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.impl.file;


import com.google.common.base.Strings;
import java.nio.file.Path;
import org.apache.commons.lang3.tuple.Pair;

public class FileUtil {

  private static final int DELIMITER_CHAR = 46;

  public static Pair<String, String> separateExtension(Path path) {
    return separateExtension(path.getFileName().toString());
  }

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
}
