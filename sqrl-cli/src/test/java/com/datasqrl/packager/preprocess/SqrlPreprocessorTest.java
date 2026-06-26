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
package com.datasqrl.packager.preprocess;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.SharedScriptConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.FilePreprocessingPipeline;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SqrlPreprocessorTest {

  @Mock private PackageJson packageJson;
  @Mock private PackageJson.ScriptConfig scriptConfig;

  @TempDir private Path tempDir;

  private SqrlPreprocessor underTest;
  private Path sourceDir;
  private Path buildDir;
  private FilePreprocessingPipeline.Context context;

  @BeforeEach
  void setUp() throws IOException {
    sourceDir = tempDir.resolve("source");
    buildDir = tempDir.resolve("build");
    Files.createDirectories(sourceDir);
    Files.createDirectories(buildDir);

    context =
        new FilePreprocessingPipeline.Context(
            sourceDir,
            buildDir,
            tempDir.resolve("lib"),
            tempDir.resolve("data"),
            ErrorCollector.root());

    underTest = new SqrlPreprocessor(packageJson, ErrorCollector.root());
  }

  @Test
  void givenNonSqrlFile_whenProcess_thenSkipsFile() throws IOException {
    var sqlFile = sourceDir.resolve("schema.sql");
    Files.writeString(sqlFile, "SELECT 1;");

    underTest.process(sqlFile, context);

    assertThat(buildDir.resolve("schema.sql")).doesNotExist();
    verifyNoInteractions(packageJson);
  }

  @Test
  void givenSqrlFileWithoutTemplateValues_whenProcess_thenCopiesFileUnchanged() throws IOException {
    givenProjectConfig(Map.of());
    var sqrlFile = sourceDir.resolve("script.sqrl");
    var content = "IMPORT {{tableName}};";
    Files.writeString(sqrlFile, content);

    underTest.process(sqrlFile, context);

    assertThat(Files.readString(buildDir.resolve("script.sqrl"))).isEqualTo(content);
  }

  @Test
  void givenSqrlFileWithProjectTemplateValues_whenProcess_thenRendersTemplate() throws IOException {
    givenProjectConfig(Map.of("tableName", "Orders", "limit", 10));
    var sqrlFile = sourceDir.resolve("script.sqrl");
    Files.writeString(sqrlFile, "IMPORT {{tableName}}; LIMIT {{limit}};");

    underTest.process(sqrlFile, context);

    assertThat(Files.readString(buildDir.resolve("script.sqrl")))
        .isEqualTo("IMPORT Orders; LIMIT 10;");
  }

  @Test
  void givenSqrlFileInSharedScriptDirectory_whenProcess_thenRendersSharedPackageValues()
      throws IOException {
    givenSharedConfig("shared-catalog", "shared", Map.of("tableName", "OverrideOrders"));

    var sharedDir = sourceDir.resolve("shared");
    Files.createDirectories(sharedDir);
    Files.writeString(
        sharedDir.resolve("package.json"),
        """
        {
          "version": "1",
          "script": {
            "config": {
              "tableName": "SharedOrders",
              "limit": 25
            }
          }
        }
        """);
    var sqrlFile = sharedDir.resolve("catalog.sqrl");
    Files.writeString(sqrlFile, "IMPORT {{tableName}}; LIMIT {{limit}};");

    underTest.process(sqrlFile, context);

    assertThat(Files.readString(buildDir.resolve("shared/catalog.sqrl")))
        .isEqualTo("IMPORT OverrideOrders; LIMIT 25;");
  }

  @Test
  void givenSqrlFileBelowSharedScriptPath_whenProcess_thenRendersSharedPackageValues()
      throws IOException {
    givenSharedConfig("shared-catalog", "shared", Map.of("limit", 50));

    var nestedSharedDir = sourceDir.resolve("shared/nested");
    Files.createDirectories(nestedSharedDir);
    Files.writeString(
        nestedSharedDir.resolve("package.json"),
        """
        {
          "version": "1",
          "script": {
            "config": {
              "tableName": "NestedOrders",
              "limit": 25
            }
          }
        }
        """);
    var sqrlFile = nestedSharedDir.resolve("catalog.sqrl");
    Files.writeString(sqrlFile, "IMPORT {{tableName}}; LIMIT {{limit}};");

    underTest.process(sqrlFile, context);

    assertThat(Files.readString(buildDir.resolve("shared/nested/catalog.sqrl")))
        .isEqualTo("IMPORT NestedOrders; LIMIT 50;");
  }

  private void givenProjectConfig(Map<String, Object> templateValues) {
    when(packageJson.getScriptConfig()).thenReturn(scriptConfig);
    when(scriptConfig.getSharedScriptConfigs()).thenReturn(List.of());
    when(scriptConfig.getConfig()).thenReturn(templateValues);
  }

  private void givenSharedConfig(String name, String path, Map<String, Object> templateOverrides) {
    var sharedScript = mock(SharedScriptConfig.class);
    when(sharedScript.getName()).thenReturn(name);
    when(sharedScript.getPath()).thenReturn(path);
    when(sharedScript.getConfig()).thenReturn(templateOverrides);
    when(packageJson.getScriptConfig()).thenReturn(scriptConfig);
    when(scriptConfig.getSharedScriptConfigs()).thenReturn(List.of(sharedScript));
  }
}
