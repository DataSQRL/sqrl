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

import static com.datasqrl.util.FileCompression.SUPPORTED_COMPRESSION_EXTENSIONS;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public record FilenameAnalyzer(Pattern filePattern) {

  public Optional<Components> analyze(Path file) {
    if (!Files.isRegularFile(file)) {
      return Optional.empty();
    }
    return analyze(file.getFileName().toString());
  }

  public Optional<Components> analyze(String fileName) {
    var matcher = filePattern.matcher(fileName);
    if (matcher.matches()) {
      return Optional.of(
          new FilenameAnalyzer.Components(
              matcher.group(1),
              matcher.group(2).toLowerCase(),
              matcher.group(4) == null ? "" : matcher.group(4).toLowerCase()));
    }
    return Optional.empty();
  }

  public static FilenameAnalyzer of(Set<String> fileExtensions) {
    List<String> escapedExtensions =
        fileExtensions.stream().map(String::toLowerCase).map(Pattern::quote).toList();
    var pattern =
        Pattern.compile(
            "(.*)\\.("
                + String.join("|", escapedExtensions)
                + ")"
                + "(\\.("
                + String.join("|", SUPPORTED_COMPRESSION_EXTENSIONS)
                + "))?$",
            Pattern.CASE_INSENSITIVE);
    return new FilenameAnalyzer(pattern);
  }

  public record Components(String filename, String extension, String compression) {
    public boolean hasCompression() {
      return !compression.isBlank();
    }

    public String getSuffix() {
      String result = "." + extension;
      if (hasCompression()) {
        result += "." + compression;
      }
      return result;
    }
  }
}
