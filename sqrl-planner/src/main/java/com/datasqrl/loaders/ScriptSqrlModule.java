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
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.plan.MainScript;
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
import org.apache.flink.sql.parser.ddl.table.SqlCreateTable;

@RequiredArgsConstructor
public class ScriptSqrlModule implements SqrlModule {

  private final String scriptContent;
  private final Path scriptPath;
  private final NamePath namePath;
  private final SqrlStatementParser sqrlStatementParser;
  private final ModuleLoader moduleLoader;
  private final ErrorCollector errors;

  private Map<Name, Supplier<NamespaceObject>> tables = null;

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
    if (tables != null) {
      return;
    }

    tables = new HashMap<>();

    var scriptErrors = errors.withScript(scriptPath, scriptContent);
    var parsedCreateTables = sqrlStatementParser.parseCreateTables(scriptContent, scriptErrors);

    for (var parsedCreateTable : parsedCreateTables) {
      var createTableStmt = (SqrlCreateTableStatement) parsedCreateTable.statement().get();

      var sql = createTableStmt.toSql();
      var sqlNode = FlinkCalciteParser.parseSql(sql);

      if (!(sqlNode instanceof SqlCreateTable createTable)) {
        throw new IllegalStateException("Expected SqlCreateTable but got " + sqlNode.getClass());
      }

      var tableName = createTable.getName().getSimple();

      // Wrap to a supplier so validation is only triggerred when the table is referenced
      Supplier<NamespaceObject> nsObjectSupplier =
          () -> {
            var finalLocation =
                createTableStmt.mapSqlLocation(parsedCreateTable.statement().getFileLocation());

            scriptErrors
                .atFile(finalLocation)
                .checkFatal(
                    !createTable.getProperties().isEmpty(),
                    ErrorCode.INVALID_INDIVIDUAL_SCRIPT_TABLE,
                    "Referenced table '%s' is not an external table",
                    tableName);

            return new FlinkTableNamespaceObject(
                new FlinkTable(Name.system(tableName), sql, scriptPath),
                moduleLoader.getSchemaLoader());
          };

      tables.put(Name.system(tableName), nsObjectSupplier);
    }
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
