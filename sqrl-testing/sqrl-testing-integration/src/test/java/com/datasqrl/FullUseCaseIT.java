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

import com.datasqrl.AbstractAssetSnapshotTest.TestNameModifier;
import com.datasqrl.tests.DuckdbTestExtension;
import com.datasqrl.tests.IcebergTestExtension;
import com.datasqrl.tests.SnowflakeTestExtension;
import com.datasqrl.util.ArgumentsProviders;
import com.datasqrl.util.TestShardingExtension;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

/**
 * Runs and Tests the uses cases in `resources/usecases`. This stands up the entire compiled
 * pipeline and runs or tests the pipeline. Add use cases to the folder above to validate full
 * pipeline execution.
 *
 * <p>Note, that this test is resource intensive and slow.
 */
@Slf4j
@ExtendWith({
  // Keep sharding first so skipped invocations do not trigger use-case setup.
  TestShardingExtension.class,
  DuckdbTestExtension.class,
  IcebergTestExtension.class,
  SnowflakeTestExtension.class
})
public class FullUseCaseIT extends AbstractFullUseCaseTest {

  private static final Path USE_CASES = Path.of("src/test/resources/usecases");

  @ParameterizedTest
  @MethodSource("specificUseCaseProvider")
  @Disabled("Intended for manual usage")
  void specificUseCase(UseCaseParam param) {
    fullUseCaseTest(param);
  }

  /** Ad-hoc debugging entry point. Change the path below to run a single use case manually. */
  static Stream<UseCaseParam> specificUseCaseProvider() {
    return Stream.of(new UseCaseParam(USE_CASES.resolve("repository").resolve("package.json")));
  }

  @ParameterizedTest
  @ArgumentsSource(UseCaseParams.class)
  void useCase(UseCaseParam param) {
    fullUseCaseTest(param);
  }

  static class UseCaseParams extends ArgumentsProviders.PackageProvider {
    UseCaseParams() {
      super(USE_CASES);
    }

    @Override
    protected String packageJsonRegex() {
      return "package.json";
    }

    @Override
    protected Function<Path, Boolean> testModifierFilter() {
      return path -> {
        var mod = TestNameModifier.of(path.getParent());
        return mod != TestNameModifier.compile && mod != TestNameModifier.disabled;
      };
    }

    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations params, ExtensionContext ctx) {
      var jsonFiles = collectPackageJsonFiles().sorted().toList();

      // Assign sequential indices and create UseCaseParam objects
      return IntStream.range(0, jsonFiles.size())
          .mapToObj(i -> new UseCaseParam(jsonFiles.get(i), "test", i))
          .map(Arguments::of);
    }
  }
}
