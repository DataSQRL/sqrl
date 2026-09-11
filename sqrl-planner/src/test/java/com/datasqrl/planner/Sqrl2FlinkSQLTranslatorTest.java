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
package com.datasqrl.planner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.schema.SchemaLoader;
import com.datasqrl.planner.hint.HintsAndDoc;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

@EnabledOnOs(OS.LINUX)
class Sqrl2FlinkSQLTranslatorTest {

  @TempDir Path workspace;

  @Test
  void givenInsertInNonDefaultCatalog_whenAnalyzed_thenRetainsIdentityAndAuthoredMetadata() {
    // Main/inline SQRL scripts forbid CREATE CATALOG. Exercise catalog resolution directly here.
    try (var translator = createTranslator()) {
      translator
          .getCatalogManager()
          .registerCatalog(
              "other_catalog", new GenericInMemoryCatalog("other_catalog", "original_db"));
      translator.executeSQL("USE CATALOG other_catalog");
      translator.createTable(
          "CREATE TABLE SourceTable (val INT) WITH ('connector'='datagen')",
          Optional.empty(),
          mock(SchemaLoader.class),
          HintsAndDoc.EMPTY);
      var view =
          translator.addView(
              "CREATE VIEW SourceView AS SELECT val FROM SourceTable",
              HintsAndDoc.EMPTY,
              ErrorCollector.root());
      var insert =
          (RichSqlInsert) translator.parseSQL("INSERT INTO Sink SELECT val FROM SourceView");
      var target = translator.getInsertTarget(insert);
      var query = insert.getSource();
      var authoredSql = RelToFlinkSql.convertToString(query);

      var analysis =
          translator.analyzeInsertQuery(
              query,
              ObjectIdentifier.of("other_catalog", "original_db", "insert_analysis"),
              HintsAndDoc.EMPTY,
              ErrorCollector.root());
      translator.executeSQL("USE CATALOG default_catalog");

      assertThat(target).isEqualTo(ObjectIdentifier.of("other_catalog", "original_db", "Sink"));
      assertThat(analysis.getOriginalSql()).isEqualTo(authoredSql);
      assertThat(RelToFlinkSql.convertToString(query))
          .contains("FROM `other_catalog`.`original_db`.`SourceView`");
      assertThat(analysis.getFromTables()).contains(view);
    }
  }

  private Sqrl2FlinkSQLTranslator createTranslator() {
    var paths = new WorkspacePaths(workspace, workspace, workspace, workspace);
    var flink = mock(FlinkStreamEngine.class);
    when(flink.getExecutionMode()).thenReturn(RuntimeExecutionMode.STREAMING);
    when(flink.getBaseConfiguration()).thenReturn(new Configuration());
    when(flink.getStreamingSpecificConfig()).thenReturn(new Configuration());
    var config = mock(CompilerConfig.class);
    when(config.predicatePushdownRules()).thenReturn(PredicatePushdownRules.DEFAULT);
    return new Sqrl2FlinkSQLTranslator(paths, flink, config);
  }

  @Test
  void givenUdfJarOpenedDuringCompile_whenClose_thenJarHandleIsReleased() throws IOException {
    var workspacePaths = new WorkspacePaths(workspace, workspace, workspace, workspace);
    var jar = writeJar(workspacePaths.getUdfPath().resolve("udf.jar"));
    var flink = mock(FlinkStreamEngine.class);
    when(flink.getExecutionMode()).thenReturn(RuntimeExecutionMode.STREAMING);
    when(flink.getBaseConfiguration()).thenReturn(new Configuration());
    when(flink.getStreamingSpecificConfig()).thenReturn(new Configuration());
    var compilerConfig = mock(CompilerConfig.class);
    when(compilerConfig.predicatePushdownRules()).thenReturn(PredicatePushdownRules.DEFAULT);

    var translator = new Sqrl2FlinkSQLTranslator(workspacePaths, flink, compilerConfig);
    assertThatThrownBy(() -> translator.addUserDefinedFunction("missing", "no.such.Udf", true))
        .isInstanceOf(Exception.class);
    assertThat(openFiles()).contains(jar);

    translator.close();

    assertThat(openFiles()).doesNotContain(jar);
  }

  private static Path writeJar(Path path) throws IOException {
    Files.createDirectories(path.getParent());
    try (var jar = new JarOutputStream(Files.newOutputStream(path))) {
      jar.putNextEntry(new JarEntry("marker.txt"));
      jar.write("marker".getBytes());
      jar.closeEntry();
    }
    return path.toRealPath();
  }

  private static Set<Path> openFiles() throws IOException {
    var open = new HashSet<Path>();
    try (var fds = Files.list(Path.of("/proc/self/fd"))) {
      for (var fd : fds.toList()) {
        try {
          open.add(Files.readSymbolicLink(fd));
        } catch (IOException ignored) {
        }
      }
    }
    return open;
  }
}
