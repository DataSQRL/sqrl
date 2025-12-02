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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.CompilerConfigImpl;
import com.datasqrl.planner.FlinkPlannerConfigBuilder;
import com.datasqrl.planner.PredicatePushdownRules;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqlScriptStatementSplitter;
import com.datasqrl.util.ArgumentsProviders;
import com.datasqrl.util.SnapshotTest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.io.FilenameUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

class PredicatePushdownTest extends AbstractAssetSnapshotTest {

  public static final Path SCRIPT_DIR = getResourcesDirectory("predicate-pushdown");

  @TempDir private Path tempDir;

  PredicatePushdownTest() {
    super(null);
  }

  @AfterEach
  @Override
  public void cleanup() throws IOException {
    super.cleanup();
    clearDir(Path.of("/tmp", "pp-tests"));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlFiles.class)
  void testPredicatePushdownRules(Path sqlFile, PredicatePushdownRules rules) throws Exception {
    executeTest(sqlFile, rules);
  }

  @Disabled("Intended for manual usage")
  @ParameterizedTest
  @EnumSource(PredicatePushdownRules.class)
  void testSingleFile(PredicatePushdownRules rules) throws Exception {
    var sqlFile = SCRIPT_DIR.resolve("multi-table-source.sql");
    assertThat(sqlFile).isRegularFile();

    executeTest(sqlFile, rules);
  }

  private void executeTest(Path sqlFile, PredicatePushdownRules rules) throws Exception {
    var stmts = parseStatements(sqlFile);
    var tEnv = initTableEnv(rules);

    // Execute SQL statements
    String planText = null;
    var it = stmts.iterator();
    while (it.hasNext()) {
      var stmt = it.next();

      if (it.hasNext()) {
        tEnv.executeSql(stmt.get());
      } else {
        // Explain the last SELECT to get the physical plan
        planText = tEnv.explainSql(stmt.get(), ExplainFormat.TEXT);
      }
    }

    assertThat(planText).isNotBlank();

    // Create snapshot
    var snapshotName =
        FilenameUtils.removeExtension(sqlFile.getFileName().toString())
            + '_'
            + rules.name().toLowerCase();

    snapshot =
        new SnapshotTest.Snapshot(
            PredicatePushdownTest.class.getName(), snapshotName, new StringBuilder(planText));

    createSnapshot();
  }

  private List<ParsedObject<String>> parseStatements(Path sqlFile) throws Exception {
    var sourceSql =
        Files.readString(sqlFile).replaceAll("__tempdir__", tempDir.toAbsolutePath().toString());

    return SqlScriptStatementSplitter.splitStatements(sourceSql);
  }

  private TableEnvironment initTableEnv(PredicatePushdownRules rules) {
    var flinkConf = new Configuration();
    flinkConf.setString("table.exec.iceberg.use-v2-sink", "true");

    var compilerConf = mock(CompilerConfigImpl.class);
    when(compilerConf.predicatePushdownRules()).thenReturn(rules);

    var tEnv = TableEnvironment.create(flinkConf);

    var configBuilder = new FlinkPlannerConfigBuilder(compilerConf, flinkConf);
    tEnv.getConfig().setPlannerConfig(configBuilder.build());

    return tEnv;
  }

  static class SqlFiles extends ArgumentsProviders.FileProvider {

    SqlFiles() {
      super(SCRIPT_DIR, ".sql");
    }

    @Override
    public Stream<? extends Arguments> provideArguments(
        ParameterDeclarations params, ExtensionContext ctx) {

      return getCollectedFiles()
          .flatMap(
              file ->
                  Arrays.stream(PredicatePushdownRules.values())
                      .sorted()
                      .map(rule -> Arguments.of(file, rule)));
    }
  }
}
