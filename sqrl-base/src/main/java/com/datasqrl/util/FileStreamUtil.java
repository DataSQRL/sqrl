/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

public class FileStreamUtil {

  public static Stream<String> filesByline(Path... paths) {
    return filesByline(Arrays.stream(paths));
  }

  public static Stream<String> filesByline(Stream<Path> paths) {
    Preconditions.checkArgument(paths != null);
    return paths.flatMap(p -> {
      try {
        return Files.lines(p);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static Stream<String> readByLine(InputStream is, String charset) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset));
    return reader.lines();
  }
}
