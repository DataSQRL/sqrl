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
package com.datasqrl.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TestRunnerConfigImpl implements TestRunnerConfiguration {

  SqrlConfig sqrlConfig;

  @Override
  public Path getSnapshotDir(Path rootDir) {
    var snapshotDir = Paths.get(sqrlConfig.asString("snapshot-dir").get());

    return combineWithRootIfRelative(rootDir, snapshotDir);
  }

  @Override
  public Optional<Path> getTestDir(Path rootDir) {
    var testDir = Paths.get(sqrlConfig.asString("test-dir").get());
    testDir = combineWithRootIfRelative(rootDir, testDir);

    return Files.isDirectory(testDir) ? Optional.of(testDir) : Optional.empty();
  }

  @Override
  public int getDelaySec() {
    return sqrlConfig.asInt("delay-sec").get();
  }

  @Override
  public int getMutationDelaySec() {
    return sqrlConfig.asInt("mutation-delay-sec").get();
  }

  @Override
  public int getRequiredCheckpoints() {
    return sqrlConfig.asInt("required-checkpoints").get();
  }

  @Override
  public Map<String, String> getHeaders() {
    return sqrlConfig.asMap("headers", String.class).getOptional().orElse(Map.of());
  }

  private Path combineWithRootIfRelative(Path rootDir, Path dir) {
    return dir.isAbsolute() ? dir : rootDir.resolve(dir);
  }
}
