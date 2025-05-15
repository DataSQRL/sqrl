/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.util;

import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;

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
        InputStreamReader inputStreamReader =
            new InputStreamReader(inputStream, StandardCharsets.UTF_8);
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
