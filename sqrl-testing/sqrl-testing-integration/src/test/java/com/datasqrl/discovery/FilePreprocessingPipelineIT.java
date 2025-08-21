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
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.FilePreprocessingPipeline;
import com.datasqrl.packager.preprocess.CopyStaticDataPreprocessor;
import com.datasqrl.packager.preprocess.JarPreprocessor;
import com.datasqrl.util.SqrlObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FilePreprocessingPipelineIT extends AbstractAssetSnapshotTest {

  public static final Path FILES_DIR = getResourcesDirectory("preprocessor-test-project");

  private ErrorCollector errorCollector;
  private BuildPath buildPath;

  public FilePreprocessingPipelineIT() {
    super(FILES_DIR.resolve("build"));
    super.buildDir = super.outputDir;
  }

  @BeforeEach
  void setup() {
    errorCollector = ErrorCollector.root();
    buildPath = new BuildPath(outputDir, outputDir);
  }

  @Test
  public void given_dataFiles_when_copyStaticDataPreprocessorRuns_then_copiesDataFilesCorrectly()
      throws Exception {
    // Arrange
    var preprocessorOrchestrator =
        new FilePreprocessingPipeline(buildPath, Set.of(new CopyStaticDataPreprocessor()));

    // Act
    preprocessorOrchestrator.run(FILES_DIR, errorCollector);

    // Assert
    Path dataDir = buildPath.getDataPath();

    // Verify JSONL file is copied to data directory unchanged
    Path jsonlFile = dataDir.resolve("sample_data.jsonl");
    assertThat(jsonlFile).exists();
    String jsonlContent = Files.readString(jsonlFile);
    assertThat(jsonlContent).contains("Alice", "Bob", "Charlie");

    // Verify CSV.GZ file is processed (header removed) and copied to data directory
    Path csvFile = dataDir.resolve("sample_data.csv.gz");
    assertThat(csvFile).exists();

    // Verify CSV header was removed by reading the processed file
    String csvContent = readGzipFile(csvFile);
    String[] lines = csvContent.split("\n");
    assertThat(lines).hasSize(2); // Should have 2 data lines, header removed
    assertThat(lines[0]).startsWith("value1"); // First line should be data, not header
    assertThat(csvContent).doesNotContain("header1,header2,header3");

    // Verify exactly 2 data files were processed
    assertThat(countFiles(dataDir, false)).isEqualTo(2);

    // Verify no errors occurred during processing
    assertThat(errorCollector.hasErrors()).isFalse();
  }

  @Test
  public void given_jarFile_when_jarPreprocessorRuns_then_processesJarCorrectly() throws Exception {
    // Arrange
    var preprocessorOrchestrator =
        new FilePreprocessingPipeline(buildPath, Set.of(new JarPreprocessor()));

    // Act
    preprocessorOrchestrator.run(FILES_DIR, errorCollector);

    // Assert
    Path libDir = buildPath.getUdfPath();

    // Verify JAR file is copied to lib directory
    Path jarFile = libDir.resolve("myjavafunction-0.1.0-snapshot.jar");
    assertThat(jarFile).exists();

    // Verify function manifest is created in build directory
    verifyFunctionManifestFiles("MyScalarFunction", "MyAsyncScalarFunction");

    // Verify lib directory has exactly 1 JAR file
    assertThat(countFiles(libDir, false)).isEqualTo(1);

    // Verify no errors occurred during processing
    assertThat(errorCollector.hasErrors()).isFalse();
  }

  @Test
  public void given_mixedFiles_when_allPreprocessorsRun_then_processesAllFilesCorrectly()
      throws Exception {
    // Arrange
    var preprocessors =
        Sets.newLinkedHashSet(List.of(new CopyStaticDataPreprocessor(), new JarPreprocessor()));
    var preprocessorOrchestrator = new FilePreprocessingPipeline(buildPath, preprocessors);

    // Act
    preprocessorOrchestrator.run(FILES_DIR, errorCollector);

    // Assert data files are processed correctly
    Path dataDir = buildPath.getDataPath();
    assertThat(dataDir.resolve("sample_data.jsonl")).exists();
    assertThat(dataDir.resolve("sample_data.csv.gz")).exists();
    assertThat(countFiles(dataDir, false)).isEqualTo(2); // jsonl + csv.gz

    // Verify CSV header removal worked
    String csvContent = readGzipFile(dataDir.resolve("sample_data.csv.gz"));
    assertThat(csvContent).doesNotContain("header1,header2,header3");

    // Assert JAR files are processed correctly
    Path libDir = buildPath.getUdfPath();
    assertThat(libDir.resolve("myjavafunction-0.1.0-snapshot.jar")).exists();
    assertThat(countFiles(libDir, false)).isEqualTo(1); // test-udf.jar

    // Verify function manifest was created and has correct content
    verifyFunctionManifestFiles("MyScalarFunction", "MyAsyncScalarFunction");

    // Assert build files are copied correctly
    assertThat(outputDir.resolve("schema.graphqls")).exists();
    assertThat(outputDir.resolve("operations").resolve("query.graphql")).exists();
    assertThat(outputDir.resolve("sub").resolve("imported.sqrl")).exists();
    assertThat(outputDir.resolve("sub").resolve("avro.avsc")).exists();
    // 4 build files + 2*2 data files (duplicated) + 1 JAR + 2 function manifest
    assertThat(countFiles(outputDir, true)).isEqualTo(11);

    // Verify filtering works - unsupported files are not copied
    assertThat(outputDir.resolve("sub").resolve("dontcopy.txt")).doesNotExist();
    assertThat(dataDir.resolve("dontcopy.txt")).doesNotExist();
    assertThat(libDir.resolve("dontcopy.txt")).doesNotExist();

    // Verify no errors occurred
    assertThat(errorCollector.hasErrors()).isFalse();
  }

  @SneakyThrows
  private void verifyFunctionManifestFiles(String... fnNames) {
    for (var fnName : fnNames) {
      Path manifestFile = outputDir.resolve(fnName + ".function.json");
      assertThat(manifestFile).exists();

      // Verify manifest content
      JsonNode manifest = SqrlObjectMapper.INSTANCE.readTree(manifestFile.toFile());
      assertThat(manifest.get("language").asText()).isEqualTo("java");
      assertThat(manifest.get("functionClass").asText()).isEqualTo("com.myudf." + fnName);
      assertThat(manifest.get("jarPath").asText()).isEqualTo("myjavafunction-0.1.0-snapshot.jar");
    }
  }

  /** Reads content from a GZIP compressed file */
  @SneakyThrows
  private String readGzipFile(Path gzipFile) {
    StringBuilder content = new StringBuilder();
    try (var gzipStream = new GZIPInputStream(Files.newInputStream(gzipFile));
        var reader = new BufferedReader(new InputStreamReader(gzipStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!content.isEmpty()) {
          content.append("\n");
        }
        content.append(line);
      }
    }
    return content.toString();
  }

  /** Counts files in a directory, optionally recursively */
  @SneakyThrows
  private long countFiles(Path path, boolean recursive) {
    if (!Files.exists(path) || !Files.isDirectory(path)) {
      return 0;
    }

    try (Stream<Path> files = recursive ? Files.walk(path) : Files.list(path)) {
      return files.filter(Files::isRegularFile).count();
    }
  }
}
