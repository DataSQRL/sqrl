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
package com.datasqrl.engine.stream.flink;

import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonType;
import com.datasqrl.flinkrunner.stdlib.json.FlinkJsonTypeSerializer;
import com.datasqrl.planner.util.NonSecretEnvVarResolver;
import com.datasqrl.sql.SqlCallRewriter;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlDistribution;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlConstraintEnforcement;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.constraint.SqlUniqueSpec;
import org.apache.flink.sql.parser.ddl.table.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.table.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.table.SqlTableLike;
import org.apache.flink.sql.parser.ddl.view.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.dml.SqlInsertConflictBehavior;
import org.apache.flink.sql.parser.type.SqlRawTypeNameSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.logical.RawType;

public class FlinkSqlNodes {

  public static final SqlDistribution NO_DISTRIBUTION = null;

  private static final String RAW_JSON = "RAW_JSON";
  private static final RawType<FlinkJsonType> RAW_JSON_TYPE =
      new RawType<>(FlinkJsonType.class, new FlinkJsonTypeSerializer());

  public static SqlIdentifier identifier(String str) {
    return new SqlIdentifier(str, SqlParserPos.ZERO);
  }

  public static SqlIdentifier identifier(ObjectIdentifier identifier) {
    return new SqlIdentifier(identifier.toList(), SqlParserPos.ZERO);
  }

  public static SqlCreateView createView(String tableName, SqlNode query) {
    return new SqlCreateView(
        SqlParserPos.ZERO,
        identifier(tableName),
        SqlNodeList.EMPTY,
        query,
        false,
        false,
        false,
        null,
        null);
  }

  public static RichSqlInsert createInsert(SqlNode source, ObjectIdentifier targetTable) {
    return createInsert(source, targetTable, Optional.empty());
  }

  public static RichSqlInsert createInsert(
      SqlNode source,
      ObjectIdentifier targetTable,
      Optional<SqlInsertConflictBehavior> conflictBehavior) {
    return new RichSqlInsert(
        SqlParserPos.ZERO,
        SqlNodeList.EMPTY,
        SqlNodeList.EMPTY,
        identifier(targetTable),
        source,
        null,
        null,
        conflictBehavior.map(cb -> cb.symbol(SqlParserPos.ZERO)).orElse(null));
  }

  public static SqlCreateFunction createFunction(String name, String clazz, boolean isSystem) {
    return createFunction(identifier(name), clazz, isSystem);
  }

  public static SqlCreateFunction createFunction(
      SqlIdentifier identifier, String clazz, boolean isSystem) {
    return new SqlCreateFunction(
        SqlParserPos.ZERO,
        identifier,
        SqlLiteral.createCharString(clazz, SqlParserPos.ZERO),
        "JAVA",
        true,
        isSystem,
        isSystem,
        SqlNodeList.EMPTY,
        SqlNodeList.EMPTY);
  }

  public static SqlWatermark createSourceWatermark(String tsCol) {
    var eventTimeColumn = identifier(tsCol);

    return createSourceWatermark(eventTimeColumn);
  }

  public static SqlWatermark createSourceWatermark(SqlIdentifier eventTimeColumn) {
    var sourceWatermarkStrategy =
        new SqlBasicCall(
            new SqlUnresolvedFunction(
                identifier("SOURCE_WATERMARK"),
                null,
                null,
                null,
                List.of(),
                SqlFunctionCategory.SYSTEM),
            List.of(),
            SqlParserPos.ZERO);

    return createWatermark(eventTimeColumn, sourceWatermarkStrategy);
  }

  public static SqlWatermark createWatermark(String tsCol, long watermarkMillis) {
    var eventTimeColumn = identifier(tsCol);
    return createWatermark(
        eventTimeColumn,
        boundedStrategy(eventTimeColumn, Double.toString(watermarkMillis / 1000d)));
  }

  public static SqlWatermark createWatermark(
      SqlIdentifier eventTimeColumn, SqlNode watermarkStrategy) {
    return new SqlWatermark(SqlParserPos.ZERO, eventTimeColumn, watermarkStrategy);
  }

  public static SqlNode boundedStrategy(SqlNode watermark, String delay) {
    return new SqlBasicCall(
        SqlStdOperatorTable.MINUS,
        List.of(
            watermark,
            SqlLiteral.createInterval(
                1,
                delay,
                new SqlIntervalQualifier(TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO),
                SqlParserPos.ZERO)),
        SqlParserPos.ZERO);
  }

  public static SqlTableConstraint createPrimaryKeyConstraint(List<String> primaryKey) {
    var pk = SqlUniqueSpec.PRIMARY_KEY.symbol(SqlParserPos.ZERO);
    var pkColumns =
        new SqlNodeList(
            primaryKey.stream().map(FlinkSqlNodes::identifier).collect(Collectors.toList()),
            SqlParserPos.ZERO);

    return new SqlTableConstraint(
        null,
        pk,
        pkColumns,
        SqlConstraintEnforcement.NOT_ENFORCED.symbol(SqlParserPos.ZERO),
        true,
        SqlParserPos.ZERO);
  }

  public static SqlCreateTable resolveTableProperties(SqlCreateTable createTable) {
    var resolvedPropMap = resolveProperties(createTable.getProperties());
    var resolvedPropList = createProperties(resolvedPropMap);

    if (createTable instanceof SqlCreateTableLike likeTable) {
      return new SqlCreateTableLike(
          likeTable.getParserPosition(),
          likeTable.getName(),
          likeTable.getColumnList(),
          likeTable.getTableConstraints(),
          resolvedPropList,
          likeTable.getDistribution(),
          createPartitionKeys(likeTable.getPartitionKeyList()),
          likeTable.getWatermark().orElse(null),
          createStringLiteral(likeTable.getComment()),
          likeTable.getTableLike(),
          likeTable.isTemporary(),
          likeTable.ifNotExists);
    }

    return new SqlCreateTable(
        createTable.getParserPosition(),
        createTable.getName(),
        createTable.getColumnList(),
        createTable.getTableConstraints(),
        resolvedPropList,
        createTable.getDistribution(),
        createPartitionKeys(createTable.getPartitionKeyList()),
        createTable.getWatermark().orElse(null),
        createStringLiteral(createTable.getComment()),
        createTable.isTemporary(),
        createTable.ifNotExists);
  }

  /**
   * Replaces the RAW_JSON column type alias with the actual RAW type used by the Flink JSON
   * functions.
   */
  public static SqlCreateTable resolveRawJsonTypAliases(SqlCreateTable createTable) {
    var columns = new ArrayList<SqlNode>(createTable.getColumnList().size());
    var changed = false;
    for (var column : createTable.getColumnList()) {
      var resolvedColumn = resolveRawJsonType(column);
      columns.add(resolvedColumn);
      changed |= resolvedColumn != column;
    }

    if (!changed) {
      return createTable;
    }

    var resolvedColumns = new SqlNodeList(columns, createTable.getColumnList().getParserPosition());
    if (createTable instanceof SqlCreateTableLike likeTable) {
      return new SqlCreateTableLike(
          likeTable.getParserPosition(),
          likeTable.getName(),
          resolvedColumns,
          likeTable.getTableConstraints(),
          createProperties(likeTable.getProperties()),
          likeTable.getDistribution(),
          createPartitionKeys(likeTable.getPartitionKeyList()),
          likeTable.getWatermark().orElse(null),
          createStringLiteral(likeTable.getComment()),
          likeTable.getTableLike(),
          likeTable.isTemporary(),
          likeTable.ifNotExists);
    }

    return new SqlCreateTable(
        createTable.getParserPosition(),
        createTable.getName(),
        resolvedColumns,
        createTable.getTableConstraints(),
        createProperties(createTable.getProperties()),
        createTable.getDistribution(),
        createPartitionKeys(createTable.getPartitionKeyList()),
        createTable.getWatermark().orElse(null),
        createStringLiteral(createTable.getComment()),
        createTable.isTemporary(),
        createTable.ifNotExists);
  }

  private static SqlNode resolveRawJsonType(SqlNode column) {
    if (column instanceof SqlRegularColumn regularColumn
        && isRawJsonType(regularColumn.getType())) {

      return new SqlRegularColumn(
          regularColumn.getParserPosition(),
          regularColumn.getName(),
          createStringLiteral(regularColumn.getComment()),
          createFlexibleJsonRawType(regularColumn.getType()),
          regularColumn.getConstraint().orElse(null));
    }

    if (column instanceof SqlMetadataColumn metadataColumn
        && isRawJsonType(metadataColumn.getType())) {

      var metadataAlias =
          metadataColumn.getMetadataAlias().map(FlinkSqlNodes::createStringLiteral).orElse(null);

      return new SqlMetadataColumn(
          metadataColumn.getParserPosition(),
          metadataColumn.getName(),
          createStringLiteral(metadataColumn.getComment()),
          createFlexibleJsonRawType(metadataColumn.getType()),
          metadataAlias,
          metadataColumn.isVirtual());
    }

    return column;
  }

  private static boolean isRawJsonType(SqlDataTypeSpec type) {
    var typeName = type.getTypeNameSpec().getTypeName();
    return typeName != null && RAW_JSON.equalsIgnoreCase(typeName.getSimple());
  }

  private static SqlDataTypeSpec createFlexibleJsonRawType(SqlDataTypeSpec originalType) {
    var originalPosition = originalType.getParserPosition();
    var rawTypeSql = RAW_JSON_TYPE.asSerializableString();

    var position =
        new SqlParserPos(
            originalPosition.getLineNum(),
            originalPosition.getColumnNum(),
            originalPosition.getLineNum(),
            originalPosition.getColumnNum() + rawTypeSql.length() - 1);

    var rawTypeName =
        new SqlRawTypeNameSpec(
            SqlLiteral.createCharString(RAW_JSON_TYPE.getOriginatingClass().getName(), position),
            SqlLiteral.createCharString(RAW_JSON_TYPE.getSerializerString(), position),
            position);

    return new SqlDataTypeSpec(rawTypeName, position).withNullable(originalType.getNullable());
  }

  public static SqlNodeList createProperties(Map<String, String> options) {
    var sqlNodes = new ArrayList<SqlNode>(options.size());

    new TreeMap<>(options)
        .forEach(
            (key, val) -> {
              var keyLiteral = createStringLiteral(key);
              var valLiteral = createStringLiteral(val);

              sqlNodes.add(new SqlTableOption(keyLiteral, valLiteral, SqlParserPos.ZERO));
            });

    return new SqlNodeList(sqlNodes, SqlParserPos.ZERO);
  }

  public static Map<String, String> resolveProperties(SqlNodeList nodeList) {
    var res = new HashMap<String, String>();

    for (var node : nodeList) {
      var option = (SqlTableOption) node;
      var keyLiteral = (SqlLiteral) option.getKey();
      var valueLiteral = (SqlLiteral) option.getValue();
      res.put(keyLiteral.toValue(), valueLiteral.toValue());
    }

    return resolveProperties(res);
  }

  public static Map<String, String> resolveProperties(Map<String, String> props) {
    var res = new TreeMap<String, String>();
    var resolver = NonSecretEnvVarResolver.builder().strict(false).build();

    props.forEach(
        (key, val) -> {
          var resolvedVal = resolver.resolve(val);
          res.put(key, resolvedVal);
        });

    return res;
  }

  public static SqlNodeList createPartitionKeys(List<String> partitionKeys) {
    List<SqlIdentifier> keys =
        partitionKeys.stream().map(FlinkSqlNodes::identifier).collect(Collectors.toList());

    return new SqlNodeList(keys, SqlParserPos.ZERO);
  }

  public static SqlCreateTable createTable(String tableName, SqlCreateTable original) {
    return new SqlCreateTable(
        original.getParserPosition(),
        identifier(tableName),
        original.getColumnList(),
        original.getTableConstraints(),
        createProperties(original.getProperties()),
        original.getDistribution(),
        createPartitionKeys(original.getPartitionKeyList()),
        original.getWatermark().orElse(null),
        createStringLiteral(original.getComment()),
        original.isTemporary(),
        original.ifNotExists);
  }

  public static SqlCreateTable createTable(
      String tableName,
      RelDataType relDataType,
      Map<String, String> connectorOptions,
      boolean isTemporary) {

    return new SqlCreateTable(
        SqlParserPos.ZERO,
        FlinkSqlNodes.identifier(tableName),
        createColumns(relDataType),
        List.of(),
        FlinkSqlNodes.createProperties(connectorOptions),
        NO_DISTRIBUTION,
        SqlNodeList.EMPTY,
        null,
        null,
        isTemporary,
        false);
  }

  public static SqlCreateTableLike createTableLike(
      String tableName, SqlCreateTable original, SqlTableLike likeClause) {
    return new SqlCreateTableLike(
        original.getParserPosition(),
        identifier(tableName),
        original.getColumnList(),
        original.getTableConstraints(),
        createProperties(original.getProperties()),
        original.getDistribution(),
        createPartitionKeys(original.getPartitionKeyList()),
        original.getWatermark().orElse(null),
        createStringLiteral(original.getComment()),
        likeClause,
        original.isTemporary(),
        original.ifNotExists);
  }

  @Nullable
  public static SqlCharStringLiteral createStringLiteral(@Nullable String comment) {
    return comment == null ? null : SqlLiteral.createCharString(comment, SqlParserPos.ZERO);
  }

  public static SqlNodeList createColumns(RelDataType relDataType) {
    return createColumns(relDataType, Collections.emptyMap(), null);
  }

  private static SqlNodeList createColumns(
      RelDataType relDataType,
      Map<String, MetadataEntry> metadataConfig,
      MetadataExpressionParser expressionParser) {
    var fieldList = relDataType.getFieldList();
    if (fieldList.isEmpty()) {
      return SqlNodeList.EMPTY;
    }
    List<SqlNode> nodes = new ArrayList<>();

    for (RelDataTypeField column : fieldList) {
      var columnName = column.getName();
      SqlNode node;

      if (metadataConfig.containsKey(columnName)) {
        var metadataEntry = metadataConfig.get(columnName);
        var attribute = metadataEntry.attribute();
        var isVirtual = metadataEntry.virtual();
        SqlNode metadataFnc;

        if (attribute.isEmpty()) {
          metadataFnc = SqlLiteral.createCharString(metadataEntry.type().get(), SqlParserPos.ZERO);
        } else {
          metadataFnc = expressionParser.parseExpression(attribute.get());
          if (metadataFnc instanceof SqlIdentifier) {
            metadataFnc = SqlLiteral.createCharString(attribute.get(), SqlParserPos.ZERO);
          } else {
            new SqlCallRewriter().performCallRewrite((SqlCall) metadataFnc);
          }
        }

        if (metadataFnc instanceof SqlCall call) {
          node = getComputedColumn(columnName, call);
        } else {
          node =
              new SqlMetadataColumn(
                  SqlParserPos.ZERO,
                  identifier(columnName),
                  null,
                  SqlDataTypeSpecBuilder.convertTypeToFlinkSpec(column.getType()),
                  metadataFnc,
                  isVirtual.orElse(false));
        }
      } else {
        node =
            new SqlRegularColumn(
                SqlParserPos.ZERO,
                identifier(columnName),
                null,
                SqlDataTypeSpecBuilder.convertTypeToFlinkSpec(column.getType()),
                null);
      }
      nodes.add(node);
    }

    return new SqlNodeList(nodes, SqlParserPos.ZERO);
  }

  public static SqlNode getComputedColumn(String columnName, SqlCall call) {
    return new SqlTableColumn.SqlComputedColumn(
        SqlParserPos.ZERO, identifier(columnName), null, call);
  }

  public static SqlSelect selectAllFromTable(SqlIdentifier tableName) {
    return new SqlSelect(
        SqlParserPos.ZERO,
        null,
        SqlNodeList.of(SqlIdentifier.STAR),
        tableName,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  // Interface for parsing expressions
  public interface MetadataExpressionParser {
    SqlNode parseExpression(String expression);
  }

  public interface MetadataEntry {

    Optional<String> type();

    Optional<String> attribute();

    Optional<Boolean> virtual();
  }
}
