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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertEquals;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.regex.Pattern;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.TestInfo;

@Slf4j
public class SnapshotTest {

  public static final String[] BASE_SNAPSHOT_DIR =
      new String[] {"src", "test", "resources", "snapshots"};
  public static final String SNAPSHOT_EXTENSION = ".txt";

  public static final String JUNIT_SNAPSHOTS = System.getenv("SQRL_JUNIT_SNAPSHOTS");

  public static void createOrValidateSnapshot(
      @NonNull String className, @NonNull String fileName, @NonNull String content) {
    new Snapshot(className, fileName, new StringBuilder(content)).createOrValidate();
  }

  private static Path getPath(String[] components) {
    return Paths.get(components[0], Arrays.copyOfRange(components, 1, components.length));
  }

  @Value
  public static class Snapshot {
    public static final String CONTENT_DELIMITER = "\n";
    public static final String HEADER_PREFIX = ">>>";
    public static final String HEADER_DELIMITER = "-";
    public static final String HEADER_SUFFIX = "\n";
    public static final String FILE_DELIMITER = "_";

    private static final Pattern PARAMETRIZED_TEST = Pattern.compile("^\\[\\d+\\] (.+)$");

    String className;
    String fileName;
    StringBuilder content;

    public static Snapshot of(@NonNull String name, @NonNull TestInfo testInfo, String content) {
      var fileName = testInfo.getDisplayName();
      var matcher = PARAMETRIZED_TEST.matcher(fileName);
      if (matcher.find()) {
        fileName = matcher.group(1);
      }
      if (fileName.endsWith("()")) {
        fileName = StringUtil.removeFromEnd(fileName, "()");
      }
      var c = new StringBuilder();
      if (!Strings.isNullOrEmpty(content)) {
        c.append(content);
      }
      return new Snapshot(name, fileName, c);
    }

    public static Snapshot of(@NonNull Class testClass, @NonNull String... testParameters) {
      Preconditions.checkArgument(testParameters.length > 0);
      var fileName = String.join(FILE_DELIMITER, testParameters);
      return new Snapshot(testClass.getName(), fileName, new StringBuilder());
    }

    public static Snapshot of(@NonNull Class testClass, @NonNull TestInfo testInfo) {
      return of(testClass.getName(), testInfo, null);
    }

    public static Snapshot of(String displayName, Class<?> testClass) {
      return new Snapshot(testClass.getName(), displayName, new StringBuilder());
    }

    public String getContent() {
      return content.toString();
    }

    public boolean hasContent() {
      return content.length() > 0;
    }

    public Snapshot addContent(@NonNull String addedContent, String... caseNames) {
      if (caseNames != null && caseNames.length > 0) {
        // Add header
        var j = 0;
        for (String caseName : caseNames) {
          if (j++ == 0) {
            content.append(HEADER_PREFIX);
          } else {
            content.append(HEADER_DELIMITER);
          }
          content.append(caseName);
        }
        content.append(HEADER_SUFFIX);
      }
      content.append(addedContent).append(CONTENT_DELIMITER);
      return this;
    }

    @SneakyThrows
    public void createOrValidate() {
      String content = getContent();
      Preconditions.checkArgument(
          fileName.matches("^[a-zA-Z0-9_-]+$"), "Invalid display name: %s", fileName);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(className), "No snapshot class name");
      Preconditions.checkArgument(!Strings.isNullOrEmpty(content), "No snapshot content");

      String[] snapLocation = ArrayUtils.addAll(BASE_SNAPSHOT_DIR, className.split("\\."));
      snapLocation = ArrayUtils.addAll(snapLocation, fileName + SNAPSHOT_EXTENSION);
      Path path = getPath(snapLocation);
      if (!Files.exists(path) || updateSnapshotProfile()) {
        Files.createDirectories(path.getParent());
        log.info("Test not running, creating snapshot");
        Files.write(path, content.getBytes());
        fail("Creating snapshots: " + "file://" + path.toFile().getAbsolutePath());
      } else {
        String expected = Files.readString(path);

        if (JUNIT_SNAPSHOTS != null) {
          /*
          Intellij works much better with JUNIT based assertions, but on the build server
          the assertThat output is easier to analyze.
          */
          assertEquals(
              "Mismatched snapshots: "
                  + fileName
                  + " "
                  + "file://"
                  + path.toFile().getAbsolutePath(),
              expected,
              content);
        } else {
          var result = path.getParent().resolve("build").resolve(path.getFileName());
          Files.createDirectories(result.getParent());
          Files.write(result, content.getBytes());
          assertThat(result).hasContent(expected);
        }
      }
    }

    @SneakyThrows
    private boolean updateSnapshotProfile() {
      return Boolean.parseBoolean(System.getProperty("snapshots.update", "false"));
    }
  }
}
