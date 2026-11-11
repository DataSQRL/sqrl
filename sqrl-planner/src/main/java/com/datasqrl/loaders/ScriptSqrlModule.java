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
package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.stream.flink.FlinkCalciteParser;
import com.datasqrl.engine.stream.flink.FlinkSqlNodes;
import com.datasqrl.error.CollectedException;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.plan.MainScript;
import com.datasqrl.planner.parser.ParsePosUtil;
import com.datasqrl.planner.parser.SqrlCreateTableStatement;
import com.datasqrl.planner.parser.SqrlStatementParser;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.table.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.table.SqlCreateTableLike;

@RequiredArgsConstructor
public class ScriptSqrlModule implements SqrlModule {

  private final String scriptContent;
  private final Path scriptPath;
  private final NamePath namePath;
  private final SqrlStatementParser sqrlStatementParser;
  private final ModuleLoader moduleLoader;
  private final ErrorCollector errors;

  private Map<Name, Supplier<NamespaceObject>> tables = null;

  /**
   * The error of parsing the script for its tables, which every lookup fails with, so that it is
   * reported once
   */
  private CollectedException parseError = null;

  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    initTables();

    return Optional.ofNullable(tables.get(name)).map(Supplier::get);
  }

  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    return List.of(new ScriptNamespaceObject(true));
  }

  public NamespaceObject asNamespaceObject() {
    return new ScriptNamespaceObject(false);
  }

  /**
   * Initializes all {@code CREATE TABLE} statements defined in the scrip for individual table
   * imports and exports.
   *
   * <p>The script is parsed eagerly so table names can be resolved through the module namespace,
   * but validation and construction only run when that specific table is referenced.
   */
  private void initTables() {
    if (parseError != null) {
      throw parseError;
    }
    if (tables != null) {
      return;
    }

    try {
      tables = parseTables();
    } catch (CollectedException e) {
      parseError = e;
      throw e;
    }
  }

  /**
   * Parses the {@code CREATE TABLE} statements of the script. Like a statement of the script that
   * fails to parse, one that the Flink parser rejects fails all tables, after the errors of all of
   * them are reported at their position in the script.
   */
  private Map<Name, Supplier<NamespaceObject>> parseTables() {
    var scriptErrors = errors.withScript(scriptPath, scriptContent);
    var parsedCreateTables = sqrlStatementParser.parseCreateTables(scriptContent, scriptErrors);

    var tables = new HashMap<Name, Supplier<NamespaceObject>>();
    RuntimeException firstError = null;
    for (var parsedCreateTable : parsedCreateTables) {
      var createTableStmt = (SqrlCreateTableStatement) parsedCreateTable.statement().get();

      SqlNode sqlNode;
      try {
        sqlNode = FlinkCalciteParser.parseSql(createTableStmt.toSql());
      } catch (RuntimeException e) {
        var error =
            ParsePosUtil.reportError(
                scriptErrors.atFile(parsedCreateTable.statement().getFileLocation()),
                e,
                createTableStmt::mapSqlLocation);
        if (firstError == null) {
          firstError = error;
        }
        continue;
      }

      if (!(sqlNode instanceof SqlCreateTable createTable)) {
        throw new IllegalStateException("Expected SqlCreateTable but got " + sqlNode.getClass());
      }

      var tableName = createTable.getName().getSimple();

      // Wrap to a supplier so validation is only triggerred when the table is referenced
      Supplier<NamespaceObject> nsObjectSupplier =
          () -> {
            var sourceLocation =
                parsedCreateTable
                    .statement()
                    .getFileLocation()
                    .add(createTableStmt.getCreateTable().getFileLocation());

            var isExternalTable =
                FlinkSqlNodes.resolveProperties(createTable.getProperties())
                        .containsKey("connector")
                    || createTable instanceof SqlCreateTableLike;

            return new FlinkTableNamespaceObject(
                new FlinkTable(
                    Name.system(tableName),
                    createTableStmt,
                    scriptPath,
                    isExternalTable,
                    scriptContent,
                    sourceLocation),
                moduleLoader.getSchemaLoader());
          };

      tables.put(Name.system(tableName), nsObjectSupplier);
    }

    if (firstError != null) {
      throw firstError;
    }
    return tables;
  }

  @AllArgsConstructor
  public class ScriptNamespaceObject implements NamespaceObject {

    @Getter boolean inline;

    @Override
    public Name name() {
      return namePath.getLast();
    }

    public ModuleLoader getModuleLoader() {
      return moduleLoader;
    }

    public MainScript getScript() {
      return new MainScript() {
        @Override
        public Optional<Path> getPath() {
          return Optional.of(scriptPath);
        }

        @Override
        public String getContent() {
          return scriptContent;
        }
      };
    }
  }
}
