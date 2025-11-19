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
package com.datasqrl;

import static com.datasqrl.config.SqrlConstants.BUILD_DIR_NAME;
import static com.datasqrl.config.SqrlConstants.PACKAGE_JSON;

import com.datasqrl.util.TestShardingExtension;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Runs and Tests the uses cases in `resources/usecases`. This stands up the entire compiled
 * pipeline and runs or tests the pipeline. Add use cases to the folder above to validate full
 * pipeline execution.
 *
 * <p>Note, that this test is resource intensive and slow.
 */
@Slf4j
@ExtendWith({MiniClusterExtension.class, TestShardingExtension.class})
public class FullUseCaseIT extends AbstractFullUseCaseTest {

  private static final Path USE_CASES = Path.of("src/test/resources/usecases");

  private static final Set<String> DISABLED_USE_CASE_PATHS =
      Set.of(
          "iceberg-export", // fails in build server
          "snowflake", // fails in build server
          "sensors-full", // flaky (too much data)
          "flink-only", // not a full test case
          "multi-batch", // not a full test case
          "connectors" // not an executable test case
          );

  @Disabled("Intended for manual usage")
  @Test
  void specificUseCase() {
    var pkg = USE_CASES.resolve("function-translation").resolve("postgres").resolve("package.json");

    var param = new UseCaseParam(pkg);
    fullUseCaseTest(param);
  }

  @ParameterizedTest
  @MethodSource("nonDisabledUseCaseProvider")
  void useCase(UseCaseParam param) {
    fullUseCaseTest(param);
  }

  @SneakyThrows
  private static Set<UseCaseParam> nonDisabledUseCaseProvider() {
    var useCasesDir = USE_CASES.toAbsolutePath();

    try (var useCaseStream = Files.list(useCasesDir)) {
      var sortedPaths =
          useCaseStream
              .filter(Files::isDirectory)
              .flatMap(FullUseCaseIT::collectPackageJsonFiles)
              .sorted() // Ensure consistent ordering for index assignment
              .toList();

      // Assign sequential indices and create UseCaseParam objects
      return IntStream.range(0, sortedPaths.size())
          .mapToObj(i -> new UseCaseParam(sortedPaths.get(i), "test", i))
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }

  /**
   * Collect files that match the {@code package.json} pattern from a given use case dir recursively
   * that are not listed in {@code DISABLED_USE_CASE_PATHS}.
   */
  @SneakyThrows
  private static Stream<Path> collectPackageJsonFiles(Path useCaseDir) {
    var useCaseName = useCaseDir.getFileName().toString();

    // Skip disabled use cases entirely
    if (DISABLED_USE_CASE_PATHS.contains(useCaseName)) {
      return Stream.empty();
    }

    try (var stream = Files.walk(useCaseDir, 2)) {
      return stream
          .filter(Files::isRegularFile)
          .filter(p -> !DISABLED_USE_CASE_PATHS.contains(p.getParent().getFileName().toString()))
          .filter(p -> !BUILD_DIR_NAME.equals(p.getParent().getFileName().toString()))
          .filter(p -> PACKAGE_JSON.equals(p.getFileName().toString()))
          .toList()
          .stream();
    }
  }
}
