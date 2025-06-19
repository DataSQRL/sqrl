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
package com.datasqrl.packager;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.packager.util.FileHash;
import java.io.InputStream;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
class FileHashTest {

  @Test
  @SneakyThrows
  void fileHash() {
    InputStream is =
        Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("package-configtest.json");
    assertThat(FileHash.getFor(is)).isEqualTo("6798e72912ab81cac1b0a9b713175730");
  }

  @Test
  @Disabled
  @SneakyThrows
  void generateHash() {
    System.out.println(
        FileHash.getFor(
            Path.of(
                "/Users/matthias/git/sqrl-repository/testdata/8_e-WN-FzfckOREZ85JY6pv6ktQ.zip")));
  }
}
