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
import com.datasqrl.planner.parser.SqlScriptStatementSplitter;
import com.datasqrl.util.ArgumentsProviders;
import com.datasqrl.util.SnapshotTest;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.io.FilenameUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.ExplainFormat;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

class PredicatePushdownTest extends AbstractAssetSnapshotTest {

  public static final Path SCRIPT_DIR = getResourcesDirectory("predicate-pushdown");

  PredicatePushdownTest() {
    super(null);
  }

  @ParameterizedTest
  @ArgumentsSource(SqlFiles.class)
  void testPredicatePushdownRules(Path sqlFile, PredicatePushdownRules rules) throws Exception {
    var flinkConf = new Configuration();
    var compilerConf = mock(CompilerConfigImpl.class);
    when(compilerConf.predicatePushdownRules()).thenReturn(rules);

    var configBuilder = new FlinkPlannerConfigBuilder(compilerConf, flinkConf);

    var tEnv = TableEnvironment.create(flinkConf);
    tEnv.getConfig().setPlannerConfig(configBuilder.build());

    var sourceSql = Files.readString(sqlFile);
    var stmts = SqlScriptStatementSplitter.splitStatements(sourceSql);

    String planText = null;
    var it = stmts.iterator();
    while (it.hasNext()) {
      var stmt = it.next();

      if (it.hasNext()) {
        tEnv.executeSql(stmt.get());
      } else {
        // Explain the last SELECT
        planText = tEnv.explainSql(stmt.get(), ExplainFormat.TEXT, ExplainDetail.CHANGELOG_MODE);
      }
    }

    assertThat(planText).isNotBlank();

    var snapshotName =
        FilenameUtils.removeExtension(sqlFile.getFileName().toString())
            + '_'
            + rules.name().toLowerCase();

    snapshot =
        new SnapshotTest.Snapshot(
            PredicatePushdownTest.class.getName(), snapshotName, new StringBuilder(planText));

    createSnapshot();
  }

  @Override
  public Predicate<Path> getPlanDirFilter() {
    return path -> false;
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
