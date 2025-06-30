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
package com.datasqrl.discovery.file;

import static com.datasqrl.discovery.file.FileCompression.SUPPORTED_COMPRESSION_EXTENSIONS;

import com.google.common.base.Preconditions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

@Value
public class FilenameAnalyzer {

  Pattern filePattern;

  public Optional<Components> analyze(Path file) {
    if (!Files.isRegularFile(file)) {
      return Optional.empty();
    }
    return analyze(file.getFileName().toString());
  }

  public Optional<Components> analyze(String file) {
    var matcher = filePattern.matcher(file);
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
    Preconditions.checkArgument(
        fileExtensions.stream().allMatch(StringUtils::isAllLowerCase),
        "File extensions must be lowercase: %",
        fileExtensions);
    var pattern =
        Pattern.compile(
            "(.*)\\.("
                + String.join("|", fileExtensions)
                + ")"
                + "(\\.("
                + String.join("|", SUPPORTED_COMPRESSION_EXTENSIONS)
                + "))?$",
            Pattern.CASE_INSENSITIVE);
    return new FilenameAnalyzer(pattern);
  }

  @Value
  public static class Components {
    String filename;
    String extension;
    String compression;
  }
}
