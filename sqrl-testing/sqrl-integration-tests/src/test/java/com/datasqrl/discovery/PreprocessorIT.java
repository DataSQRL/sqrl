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

import static com.datasqrl.discovery.CopyStaticDataPreprocessorIT.countFilesRecursively;
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

public class PreprocessorIT extends AbstractAssetSnapshotTest {

  public static final Path FILES_DIR = getResourcesDirectory("preprocessor_testproject");

  PackageJson packageJson;
  ErrorCollector errorCollector = ErrorCollector.root();

  public PreprocessorIT() {
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

    long buildFilesCount = countFilesRecursively(outputDir);

    assertThat(countFilesRecursively(buildPath.getUdfPath())).isEqualTo(1);
    assertThat(countFilesRecursively(buildPath.getDataPath())).isEqualTo(0);
    // We expect another copy of the data in the build directory
    assertThat(buildFilesCount).isEqualTo(6);
  }
}
