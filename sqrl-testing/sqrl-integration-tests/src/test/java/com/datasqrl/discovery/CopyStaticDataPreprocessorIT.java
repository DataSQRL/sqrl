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
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.discovery.FlexibleSchemaInferencePreprocessorTest.DataFiles;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.preprocess.CopyStaticDataPreprocessor;
import com.datasqrl.packager.preprocessor.Preprocessor.ProcessorContext;
import com.datasqrl.util.FileHash;
import com.datasqrl.util.SnapshotTest.Snapshot;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class CopyStaticDataPreprocessorIT extends AbstractAssetSnapshotTest {

  public static final Path FILES_DIR = getResourcesDirectory("discoveryfiles");

  CopyStaticDataPreprocessor preprocessor = new CopyStaticDataPreprocessor();

  protected CopyStaticDataPreprocessorIT() {
    super(FILES_DIR.resolve("output"));
    super.buildDir = super.outputDir;
  }

  @ParameterizedTest
  @ArgumentsSource(DataFiles.class)
  @SneakyThrows
  void scripts(Path file) {
    assertThat(file).exists();
    String filename = file.getFileName().toString();
    assertThat(filename).matches(preprocessor.getPattern());
    this.snapshot = Snapshot.of(getDisplayName(file), getClass());
    preprocessor.processFile(
        file, new ProcessorContext(outputDir, buildDir, null), ErrorCollector.root());
    Path copyFile = outputDir.resolve(SqrlConstants.DATA_DIR).resolve(filename);
    assertThat(copyFile).exists().isRegularFile();
    snapshot.addContent(FileHash.getFor(copyFile), filename);
    snapshot.createOrValidate();
  }
}
