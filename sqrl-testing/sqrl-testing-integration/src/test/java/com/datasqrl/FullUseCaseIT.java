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
import com.datasqrl.util.ArgumentsProviders;
import com.datasqrl.util.TestShardingExtension;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

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

  @Test
  @Disabled("Intended for manual usage")
  void specificUseCase() {
    var pkg = USE_CASES.resolve("stdlib-to-append-stream").resolve("package.json");

    var param = new UseCaseParam(pkg);
    fullUseCaseTest(param);
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
