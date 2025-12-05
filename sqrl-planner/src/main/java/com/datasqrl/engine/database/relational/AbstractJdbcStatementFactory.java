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
package com.datasqrl.engine.database.relational;

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.calcite.OperatorRuleTransformer;
import com.datasqrl.calcite.convert.RelToSqlNode;
import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.calcite.dialect.postgres.SqlCreatePostgresView;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.CreateTableDdlFactory;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import com.datasqrl.engine.database.relational.JdbcStatement.Field;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.database.relational.ddl.statements.GenericCreateViewDdlFactory;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.planner.hint.DataTypeHint;
import com.datasqrl.planner.hint.PartitionKeyHint;
import com.datasqrl.planner.hint.PlannerHints;
import com.datasqrl.planner.hint.TtlHint;
import com.datasqrl.planner.parser.SqrlStatementParser;
import com.datasqrl.sql.DatabaseExtension;
import com.datasqrl.util.CalciteUtil;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@AllArgsConstructor
public abstract class AbstractJdbcStatementFactory implements JdbcStatementFactory {

  private static final Pattern POSITIONAL_ARG_PATTERN =
      Pattern.compile(Pattern.quote(SqrlStatementParser.POSITIONAL_ARGUMENT_PREFIX) + "(\\d+)");

  protected final OperatorRuleTransformer dialectCallConverter;
  protected final RelToSqlNode relToSqlConverter;
  protected final SqlNodeToString sqlNodeToString;
  protected final CreateTableDdlFactory createTableDdlFactory;

  @Override
  public QueryResult createQuery(
      Query query, boolean withView, Map<String, JdbcEngineCreateTable> tableIdMap) {

    return createQueryInternal(
        query.function().getSimpleName(),
        query.relNode(),
        withView,
        getTableNameMapping(tableIdMap),
        query.function().getDocumentation());
  }

  protected static Map<String, String> getTableNameMapping(
      Map<String, JdbcEngineCreateTable> tableIdMap) {
    return tableIdMap.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().tableName()));
  }

  @Override
  public QueryResult createPassthroughQuery(Query query, boolean withView) {
    var passthroughSql = query.function().getFunctionAnalysis().getOriginalSql();
    var description = query.function().getDocumentation().orElse(null);

    // Replace all argument references
    var formattedSql =
        POSITIONAL_ARG_PATTERN
            .matcher(passthroughSql)
            .replaceAll(matchResult -> replacePassthroughArg(matchResult, passthroughSql));

    var qBuilder = ExecutableJdbcReadQuery.builder().sql(formattedSql);
    if (!withView) {
      return new QueryResult(qBuilder, null);
    }

    var viewName = query.function().getSimpleName();
    var rowType = query.relNode().getRowType();
    var viewSql =
        new GenericCreateViewDdlFactory()
            .createView(viewName, rowType.getFieldNames(), passthroughSql);
    var view =
        new GenericJdbcStatement(
            viewName,
            Type.VIEW,
            viewSql,
            description,
            rowType,
            getColumns(rowType.getFieldList(), PlannerHints.EMPTY));

    return new QueryResult(qBuilder, view);
  }

  protected QueryResult createQueryInternal(
      String viewName,
      RelNode relNode,
      boolean withView,
      Map<String, String> tableNameMapping,
      Optional<String> documentation) {
    var rewrittenRelNode = dialectCallConverter.convert(relNode);
    var sqlNodes = relToSqlConverter.convert(rewrittenRelNode, tableNameMapping);
    var sql = sqlNodeToString.convert(sqlNodes).getSql();
    var qBuilder = ExecutableJdbcReadQuery.builder();
    qBuilder.sql(sql);

    JdbcStatement view = null;
    if (withView) {
      view = getViewStatement(viewName, relNode.getRowType(), sqlNodes, documentation);
    }
    return new JdbcStatementFactory.QueryResult(qBuilder, view);
  }

  @Override
  public JdbcStatement createTable(JdbcEngineCreateTable createTable) {
    var tableName = createTable.tableName();
    List<String> primaryKeys = createTable.table().getPrimaryKey().orElseThrow();
    List<String> partitionKeys =
        createTable
            .tableAnalysis()
            .getHints()
            .getHint(PartitionKeyHint.class)
            .map(PartitionKeyHint::getColumnNames)
            .orElse(List.of());
    // DagPlanner validates that partition keys are part of primary key

    PartitionType partitionType = getPartitionType(createTable, partitionKeys);
    Duration ttl =
        createTable
            .tableAnalysis()
            .getHints()
            .getHint(TtlHint.class)
            .flatMap(TtlHint::getTtl)
            .orElse(Duration.ZERO);

    return new CreateTableJdbcStatement(
        tableName,
        null,
        getColumns(createTable.datatype().getFieldList(), createTable.tableAnalysis().getHints()),
        primaryKeys,
        partitionKeys,
        partitionType,
        partitionType == PartitionType.NONE ? 0 : 1,
        ttl,
        createTable,
        createTableDdlFactory);
  }

  protected PartitionType getPartitionType(
      JdbcEngineCreateTable createTable, List<String> partitionKey) {
    return partitionKey.isEmpty() ? PartitionType.NONE : PartitionType.LIST;
  }

  protected List<Field> getColumns(List<RelDataTypeField> fields, PlannerHints hints) {
    return fields.stream().map(field -> toField(field, hints)).collect(Collectors.toList());
  }

  protected JdbcStatement.Field toField(RelDataTypeField field, PlannerHints hints) {
    var castSpec =
        getSqlType(
            field.getType(),
            hints
                .getHints(DataTypeHint.class)
                .filter(hint -> hint.getColumnIndex() == field.getIndex())
                .findFirst());
    var sqlPrettyWriter = new SqlPrettyWriter();
    castSpec.unparse(sqlPrettyWriter, 0, 0);
    var typeName = sqlPrettyWriter.toSqlString().getSql();

    var datatype = field.getType();

    return new Field(field.getName(), typeName, datatype.isNullable());
  }

  protected abstract SqlDataTypeSpec getSqlType(RelDataType type, Optional<DataTypeHint> hint);

  protected String createView(
      SqlIdentifier viewNameIdentifier, SqlNodeList columnList, SqlNode viewSqlNode) {
    var createView =
        new SqlCreatePostgresView(
            SqlParserPos.ZERO, true, viewNameIdentifier, columnList, viewSqlNode);
    return sqlNodeToString.convert(() -> createView).getSql();
  }

  public static List<String> quoteIdentifier(List<String> columns) {
    return columns.stream()
        .map(AbstractJdbcStatementFactory::quoteIdentifier)
        .collect(Collectors.toList());
  }

  public static String quoteIdentifier(String column) {
    return "\"" + column + "\"";
  }

  public static List<String> quoteIdentifiers(List<String> values) {
    return values.stream()
        .map(AbstractJdbcStatementFactory::quoteIdentifier)
        .collect(Collectors.toList());
  }

  protected Set<DatabaseExtension> extractTypeExtensions(
      Stream<RelNode> relNodes, List<DatabaseExtension> extensions) {
    return relNodes
        .map(relNode -> extractTypeExtensions(relNode, extensions))
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  protected Set<DatabaseExtension> extractTypeExtensions(
      RelNode relNode, List<DatabaseExtension> extensions) {
    Set<DatabaseExtension> matchedExtensions = new HashSet<>();
    for (RelDataTypeField field : relNode.getRowType().getFieldList()) {
      // See if we use a type from an extension by matching on class
      for (DatabaseExtension extension : extensions) {
        if (field.getType() instanceof RawRelDataType
            && ((RawRelDataType) field.getType())
                .getRawType()
                .getOriginatingClass()
                .equals(extension.typeClass())) {
          matchedExtensions.add(extension);
        }
      }
    }

    // See if we match the extension by operator
    CalciteUtil.applyRexShuttleRecursively(
        relNode,
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            for (DatabaseExtension extension : extensions) {
              for (Name functionName : extension.operators()) {
                if (functionName.equals(Name.system(call.getOperator().getName()))) {
                  matchedExtensions.add(extension);
                }
              }
            }

            return super.visitCall(call);
          }
        });

    return matchedExtensions;
  }

  private String replacePassthroughArg(MatchResult matchResult, String sql) {
    int number = Integer.parseInt(matchResult.group(1));
    checkArgument(number >= 0, "Invalid index: %s [%s]", number, sql);

    return "\\$" + (number + 1);
  }

  private JdbcStatement getViewStatement(
      String viewName, RelDataType rowType, SqlNodes sqlNodes, Optional<String> documentation) {
    var viewNameIdentifier = new SqlIdentifier(viewName, SqlParserPos.ZERO);
    var columnList =
        new SqlNodeList(
            rowType.getFieldList().stream()
                .map(f -> new SqlIdentifier(f.getName(), SqlParserPos.ZERO))
                .collect(Collectors.toList()),
            SqlParserPos.ZERO);
    var viewSql = createView(viewNameIdentifier, columnList, sqlNodes.getSqlNode());

    return new GenericJdbcStatement(
        viewName,
        Type.VIEW,
        viewSql,
        documentation.orElse(null),
        rowType,
        getColumns(rowType.getFieldList(), PlannerHints.EMPTY));
  }
}
