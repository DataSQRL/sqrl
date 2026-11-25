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
package com.datasqrl.compile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.compile.CompilerContexts.Directories;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.packager.Packager;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.planner.PlannerListener;
import com.datasqrl.planner.QueryReferences;
import com.datasqrl.planner.QueryReferences.QueryReference;
import com.datasqrl.planner.SqlScriptPlanner;
import com.datasqrl.planner.Sqrl2FlinkSQLTranslator;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.ParsedStatement;
import com.datasqrl.planner.parser.SQLStatement;
import com.datasqrl.planner.parser.SqrlAddColumnStatement;
import com.datasqrl.planner.parser.SqrlCreateTableStatement;
import com.datasqrl.planner.parser.SqrlDistinctStatement;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.util.ConfigLoaderUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.function.Predicate;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PlannerListenerTest {

  private static final String PACKAGE =
      """
      {
        "version": "1",
        "enabled-engines": ["vertx", "postgres", "kafka", "flink"],
        "script": {"main": "main.sqrl"}
      }
      """;

  private static final String SOURCES =
      """
      CREATE TABLE Events (
        id INT NOT NULL,
        amount DOUBLE,
        ts TIMESTAMP_LTZ(3) NOT NULL,
        WATERMARK FOR ts AS ts
      ) WITH (
        'connector' = 'datagen'
      );
      """;

  private static final String DISTINCT = "Latest := DISTINCT Events ON id ORDER BY ts DESC;";

  private static final String MAIN =
      """
      IMPORT sources.*;

      Big := SELECT id, amount, ts FROM Events WHERE amount > 10;
      Big.twice := amount * 2;
      %s
      ByAmount(minAmount DOUBLE NOT NULL) := SELECT * FROM Latest WHERE amount > :minAmount;
      Broken := SELECT nothing FROM Events;
      EXPORT Big TO print.Big;
      """
          .formatted(DISTINCT);

  @TempDir Path project;

  private final ErrorCollector errors = ErrorCollector.root();

  @BeforeEach
  void setUp() throws IOException {
    Files.writeString(project.resolve("package.json"), PACKAGE);
    Files.writeString(project.resolve("sources.sqrl"), SOURCES);
    Files.writeString(project.resolve("main.sqrl"), MAIN);
  }

  @Test
  void givenScript_whenPlanned_thenCallsTheListenerForStatementsTablesFunctionsAndQueries() {
    var recorder = new Recorder(statement -> false);

    // The statement that fails is skipped, and its error is thrown after the last statement
    assertThatThrownBy(() -> plan(recorder)).isInstanceOf(CollectedException.class);

    assertThat(recorder.events)
        .containsExactly(
            "start SqrlImportStatement at main.sqrl:1",
            "start SqrlCreateTableStatement at sources.sqrl:1",
            "table Events at sources.sqrl:1",
            "finish",
            "finish",
            "start SqrlTableDefinition at main.sqrl:3",
            "query",
            "table Big at main.sqrl:3",
            "finish",
            "start SqrlAddColumnStatement at main.sqrl:4",
            "reference to Big at 1:1",
            "query",
            "table Big at main.sqrl:4",
            "finish",
            "start SqrlDistinctStatement at main.sqrl:5",
            "query",
            "table Latest at main.sqrl:5",
            "finish",
            "start SqrlTableFunctionStatement at main.sqrl:6",
            "query",
            "function ByAmount at main.sqrl:6",
            "finish",
            "start SqrlTableDefinition at main.sqrl:7",
            "finish",
            "start SqrlExportStatement at main.sqrl:8",
            "reference to Big at 1:8",
            "finish");
    assertThat(errors).hasSize(1);
  }

  @Test
  void givenDistinctStatement_whenPlanned_thenReportsItsQueryOnceWithPositionsInTheStatement() {
    var recorder = new Recorder(statement -> false);

    assertThatThrownBy(() -> plan(recorder)).isInstanceOf(CollectedException.class);

    // The planner validates the statement again as SQL that it generated from the plan
    var queries = recorder.queriesOf(SqrlDistinctStatement.class);
    assertThat(queries).hasSize(1);
    assertThat(queries.get(0).references())
        .isNotEmpty()
        .allSatisfy(
            reference -> {
              var position = reference.position();
              var location =
                  queries
                      .get(0)
                      .statement()
                      .mapSqlLocation(
                          new FileLocation(position.getLineNum(), position.getColumnNum()));
              assertThat(location.line()).isEqualTo(1);
              // The columns that the validator expanded from `*` are at the table of the statement
              assertThat(DISTINCT.substring(location.offset() - 1))
                  .matches(
                      text ->
                          text.startsWith("Events")
                              || text.startsWith(Util.last(reference.column())));
            });
  }

  @Test
  void givenColumnStatement_whenPlanned_thenReportsTheQueryOfItsTableBelowTheExpression() {
    var recorder = new Recorder(statement -> false);

    assertThatThrownBy(() -> plan(recorder)).isInstanceOf(CollectedException.class);

    var queries = recorder.queriesOf(SqrlAddColumnStatement.class);
    assertThat(queries).hasSize(1);
    // `amount` in the expression is a column of the subquery. The names in the query of the table
    // follow on the line after the expression, which has one line
    assertThat(queries.get(0).references())
        .isNotEmpty()
        .allSatisfy(reference -> assertThat(reference.position().getLineNum()).isEqualTo(2));
  }

  @Test
  void givenListenerThatCancelsInAnImportedScript_whenPlanned_thenStopsWithoutAnError() {
    var recorder = new Recorder(SqrlCreateTableStatement.class::isInstance);

    assertThatThrownBy(() -> plan(recorder)).isInstanceOf(CancellationException.class);

    assertThat(recorder.events)
        .containsExactly(
            "start SqrlImportStatement at main.sqrl:1",
            "start SqrlCreateTableStatement at sources.sqrl:1",
            "finish");
    assertThat(errors).isEmpty();
  }

  /** Plans the main script like {@link CompilationProcess}, without building the physical plan. */
  private void plan(PlannerListener listener) {
    var config =
        ConfigLoaderUtils.loadUnresolvedConfig(errors, List.of(project.resolve("package.json")));
    var buildDir = project.resolve(SqrlConstants.BUILD_DIR_NAME);
    var dirs =
        new Directories(
            project, project, buildDir, buildDir, buildDir.resolve(SqrlConstants.DEPLOY_DIR_NAME));
    try (var context =
        CompilerContexts.create(errors, dirs, config, ExecutionGoal.COMPILE, false)) {
      context.getBean(Packager.class).preprocess(errors);
      var planner = context.getBean(SqlScriptPlanner.class);
      planner.setListener(listener);
      try (var translator =
          new Sqrl2FlinkSQLTranslator(
              context.getBean(WorkspacePaths.class),
              (FlinkStreamEngine) planner.getStreamStage().engine(),
              config.getCompilerConfig(),
              errors)) {
        planner.planMain(context.getBean(MainScript.class), Optional.empty(), translator);
      }
    }
  }

  private record ValidatedQuery(SQLStatement statement, List<QueryReference> references) {}

  /** Records the calls, and cancels planning at the first statement that matches. */
  private static class Recorder implements PlannerListener {

    final List<String> events = new ArrayList<>();
    final List<ValidatedQuery> queries = new ArrayList<>();
    private final Deque<SQLStatement> statements = new ArrayDeque<>();
    private final Predicate<SQLStatement> cancelAt;

    Recorder(Predicate<SQLStatement> cancelAt) {
      this.cancelAt = cancelAt;
    }

    List<ValidatedQuery> queriesOf(Class<? extends SQLStatement> statementClass) {
      return queries.stream().filter(q -> statementClass.isInstance(q.statement())).toList();
    }

    @Override
    public void statementStarted(ParsedStatement statement, ErrorLocation location) {
      var sql = statement.statement().get();
      events.add("start " + sql.getClass().getSimpleName() + " at " + describe(location));
      if (cancelAt.test(sql)) {
        throw new CancellationException("Cancelled by the test");
      }
      statements.push(sql);
    }

    @Override
    public void statementFinished() {
      statements.pop();
      events.add("finish");
    }

    @Override
    public void tableDefined(TableAnalysis table, ErrorLocation location) {
      events.add(
          "table " + table.getObjectIdentifier().getObjectName() + " at " + describe(location));
    }

    @Override
    public void functionDefined(SqrlTableFunction function, ErrorLocation location) {
      events.add("function " + function.getFullPath().getDisplay() + " at " + describe(location));
    }

    @Override
    public void tableReferenced(ObjectIdentifier table, ParsedObject<NamePath> name) {
      var location = name.getFileLocation();
      events.add(
          "reference to "
              + table.getObjectName()
              + " at "
              + location.line()
              + ":"
              + location.offset());
    }

    @Override
    public void queryValidated(SqlNode query, SqlValidator validator) {
      events.add("query");
      queries.add(new ValidatedQuery(statements.peek(), QueryReferences.collect(query, validator)));
    }

    /** The name of the file and the line of a location, e.g. {@code main.sqrl:3}. */
    private static String describe(ErrorLocation location) {
      var file =
          location.getSourcePath().map(path -> Path.of(path).getFileName().toString()).orElse("?");
      return file + ":" + location.getFileLocation().line();
    }
  }
}
