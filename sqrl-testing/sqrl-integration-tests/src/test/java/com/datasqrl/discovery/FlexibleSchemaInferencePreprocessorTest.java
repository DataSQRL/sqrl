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
package com.datasqrl.discovery;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.AbstractAssetSnapshotTest;
import com.datasqrl.config.PackageJson;
import com.datasqrl.discovery.preprocessor.FlexibleSchemaInferencePreprocessor;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrinter;
import com.datasqrl.packager.preprocessor.Preprocessor.ProcessorContext;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.SnapshotTest.Snapshot;
import com.datasqrl.util.SqrlInjector;
import com.google.inject.Guice;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class FlexibleSchemaInferencePreprocessorTest extends AbstractAssetSnapshotTest {

  public static final Path FILES_DIR = getResourcesDirectory("discoveryfiles");

  ErrorCollector errors = ErrorCollector.root();
  FlexibleSchemaInferencePreprocessor preprocessor;
  PackageJson packageJson;

  protected FlexibleSchemaInferencePreprocessorTest() {
    super(FILES_DIR.resolve("output"));
    try {
      packageJson = ConfigLoaderUtils.loadDefaultConfig(errors);
    } catch (Exception e) {
      System.out.println(ErrorPrinter.prettyPrint(errors));
      throw e;
    }
    var injector =
        Guice.createInjector(
            new SqrlInjector(
                ErrorCollector.root(),
                FILES_DIR,
                super.outputDir,
                packageJson,
                ExecutionGoal.COMPILE));
    preprocessor = injector.getInstance(FlexibleSchemaInferencePreprocessor.class);
    super.buildDir = outputDir;
  }

  @ParameterizedTest
  @ArgumentsSource(DataFiles.class)
  @SneakyThrows
  void scripts(Path file) {
    assertThat(Files.exists(file)).isTrue();
    Path targetFile = Files.copy(file, outputDir.resolve(file.getFileName()));
    String filename = file.getFileName().toString();
    assertThat(preprocessor.getPattern().matcher(filename).matches()).isTrue();
    this.snapshot = Snapshot.of(getDisplayName(file), getClass());
    preprocessor.processFile(
        targetFile, new ProcessorContext(outputDir, buildDir, packageJson), errors);
    createSnapshot();
  }

  @Override
  public Predicate<Path> getOutputDirFilter() {
    return p -> p.getFileName().toString().endsWith("table.sql");
  }

  static class DataFiles implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext)
        throws Exception {
      return Files.list(FILES_DIR).map(Arguments::of);
    }
  }
}
