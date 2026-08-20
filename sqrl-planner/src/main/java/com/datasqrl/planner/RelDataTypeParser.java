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

import static com.datasqrl.config.SqrlConstants.FLINK_DEFAULT_CATALOG;

import com.datasqrl.engine.stream.flink.FlinkSqlNodes;
import com.datasqrl.planner.parser.ParsePosUtil;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SQLStatement;
import com.datasqrl.planner.parser.StatementParserException;
import com.datasqrl.server.exec.FlinkExecFunction;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.table.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.table.SqlTableLike;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.ddl.CreateTableOperation;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class RelDataTypeParser {

  private static final String DATATYPE_PARSING_PREFIX =
      "CREATE TEMPORARY TABLE __sqrlinternal_types( ";

  private final Sqrl2FlinkSQLTranslator translator;

  /**
   * Parses a table-column definition into resolved relational data types.
   *
   * <p>Wraps the definition in a temporary {@code CREATE TABLE} statement so Flink can resolve the
   * types. Parser errors are translated to the source location of the supplied definition.
   *
   * @param dataTypeDefinition column definition and its source location
   * @return the parsed fields, including metadata and computed-column information
   */
  public List<ParsedRelDataTypeResult> parseToRelDataType(ParsedObject<String> dataTypeDefinition) {
    if (dataTypeDefinition.isEmpty()) {
      return List.of();
    }

    var createTableStatement =
        DATATYPE_PARSING_PREFIX + dataTypeDefinition.get() + " ) WITH ('connector' = 'filesystem')";

    try {
      var sqlNode = translator.parseSQL(createTableStatement);
      var op = (CreateTableOperation) translator.getOperation(sqlNode);
      var schema = op.getCatalogTable().getResolvedSchema();

      return translator.parseSchema(schema, true);
    } catch (Exception e) {
      var location = dataTypeDefinition.getFileLocation();
      var converted = ParsePosUtil.convertFlinkParserException(e);

      if (converted.isPresent()) {
        location =
            location.add(
                SQLStatement.removeFirstRowOffset(
                    converted.get().location(), DATATYPE_PARSING_PREFIX.length()));
      }

      throw new StatementParserException(
          location, e, converted.map(ParsePosUtil.MessageLocation::message).orElse(e.getMessage()));
    }
  }

  /**
   * Parses the schema of a serialized {@code CREATE TABLE} statement.
   *
   * <p>When the statement contains an unqualified {@code LIKE} source that is not in the current
   * database, resolves the source from a database created during planning before parsing.
   *
   * <p>This cross-database fallback can resolve an unintended table and must only be used for
   * well-specified cases, such as mutation database compatibility checks.
   *
   * @param createTableStatement CREATE TABLE statement whose schema is parsed
   * @return the parsed fields, including metadata and computed-column information
   */
  public List<ParsedRelDataTypeResult> parseToRelDataType(String createTableStatement) {
    var sqlNode = translator.parseSQL(createTableStatement);
    sqlNode = resolveLikeSource(sqlNode);

    var op = (CreateTableOperation) translator.getOperation(sqlNode);

    return translator.parseSchema(op.getCatalogTable().getResolvedSchema(), true);
  }

  /**
   * Qualifies an unqualified {@code LIKE} source by locating it in a database created during
   * planning.
   */
  private SqlNode resolveLikeSource(SqlNode sqlNode) {
    if (!(sqlNode instanceof SqlCreateTableLike likeTable)) {
      return sqlNode;
    }

    var likeClause = likeTable.getTableLike();
    var sourceTable = likeClause.getSourceTable();
    if (tableExists(translator.qualifyIdentifier(sourceTable))) {
      return sqlNode;
    }

    var sourceName = sourceTable.names.get(sourceTable.names.size() - 1);

    return translator.getCreatedDatabases().stream()
        .map(database -> ObjectIdentifier.of(FLINK_DEFAULT_CATALOG, database, sourceName))
        .filter(this::tableExists)
        .findFirst()
        .map(id -> modifySqlTableLike(likeTable, likeClause, id))
        .orElse(sqlNode);
  }

  private boolean tableExists(ObjectIdentifier id) {
    return translator.getCatalogManager().getTable(id).isPresent();
  }

  private SqlNode modifySqlTableLike(
      SqlCreateTableLike originalCreateTableLike, SqlTableLike likeClause, ObjectIdentifier id) {

    var modifiedLikeClause =
        new SqlTableLike(
            likeClause.getParserPosition(), FlinkSqlNodes.identifier(id), likeClause.getOptions());

    return FlinkSqlNodes.createTableLike(
        originalCreateTableLike.getName().getSimple(), originalCreateTableLike, modifiedLikeClause);
  }

  public record ParsedRelDataTypeResult(
      RelDataTypeField field, Optional<String> metadata, Optional<FlinkExecFunction> function) {}
}
