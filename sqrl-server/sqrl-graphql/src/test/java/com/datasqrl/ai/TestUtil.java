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
package com.datasqrl.ai;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import com.datasqrl.ai.util.ErrorHandling;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;

public class TestUtil {

  public static URL getResourceFile(String path) {
    URL url = TestUtil.class.getClassLoader().getResource(path);
    ErrorHandling.checkArgument(url != null, "Invalid url: %s", url);
    return url;
  }

  @SneakyThrows
  public static String getResourcesFileAsString(String path) {
    Path uriPath = Path.of(getResourceFile(path).toURI());
    return Files.readString(uriPath, StandardCharsets.UTF_8);
  }

  @SneakyThrows
  public static void snapshotTest(String result, Path pathToExpected) {
    if (Files.isRegularFile(pathToExpected)) {
      assertThat(pathToExpected).hasContent(result);
    } else {
      Files.writeString(pathToExpected, result, StandardCharsets.UTF_8, CREATE);
      fail("Created snapshot: " + pathToExpected.toAbsolutePath());
    }
  }
}
