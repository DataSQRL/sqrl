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
import com.datasqrl.config.BuildPath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.PreprocessorOrchestrator;
import com.datasqrl.util.ConfigLoaderUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.Test;

public class CopyStaticDataPreprocessorIT extends AbstractAssetSnapshotTest {

  public static final Path FILES_DIR = getResourcesDirectory("discoveryfiles");

  PackageJson packageJson;
  ErrorCollector errorCollector = ErrorCollector.root();

  public CopyStaticDataPreprocessorIT() {
    super(FILES_DIR.resolve("build"));
    super.buildDir = super.outputDir;
    packageJson = ConfigLoaderUtils.loadDefaultConfig(errorCollector);
  }

  @Test
  @SneakyThrows
  public void copyStaticData() {
    var buildPath = new BuildPath(outputDir, outputDir);
    PreprocessorOrchestrator preprocessorOrchestrator =
        new PreprocessorOrchestrator(packageJson, buildPath);
    preprocessorOrchestrator.preprocessDirectory(FILES_DIR, errorCollector);

    Path dataDir = buildPath.getDataPath();

    long dataFilesCount = countFilesRecursively(dataDir);

    long buildFilesCount = countFilesRecursively(outputDir);

    long sourceFilesCount = countFiles(FILES_DIR);

    assertThat(dataFilesCount).isEqualTo(sourceFilesCount);
    // We expect another copy of the data in the build directory
    assertThat(buildFilesCount).isEqualTo(2 * sourceFilesCount);
  }

  @SneakyThrows
  public static long countFilesRecursively(Path path) {
    if (!Files.exists(path) || !Files.isDirectory(path)) return 0;
    return Files.walk(path).filter(Files::isRegularFile).count();
  }

  @SneakyThrows
  public static long countFiles(Path path) {
    if (!Files.exists(path) || !Files.isDirectory(path)) return 0;
    return Files.list(path).filter(Files::isRegularFile).count();
  }
}
