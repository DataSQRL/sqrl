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
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.sql.SqlCallRewriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlDistribution;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlConstraintEnforcement;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.constraint.SqlUniqueSpec;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.catalog.ObjectIdentifier;

public class FlinkSqlNodeFactory {

  public static final SqlDistribution NO_DISTRIBUTION = null;

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

  public static RichSqlInsert createInsert(SqlNode source, String target) {
    return new RichSqlInsert(
        SqlParserPos.ZERO,
        SqlNodeList.EMPTY,
        SqlNodeList.EMPTY,
        identifier(target),
        source,
        null,
        null);
  }

  public static RichSqlInsert createInsert(SqlNode source, ObjectIdentifier targetTable) {
    return new RichSqlInsert(
        SqlParserPos.ZERO,
        SqlNodeList.EMPTY,
        SqlNodeList.EMPTY,
        identifier(targetTable),
        source,
        null,
        null);
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
            primaryKey.stream().map(FlinkSqlNodeFactory::identifier).collect(Collectors.toList()),
            SqlParserPos.ZERO);

    return new SqlTableConstraint(
        null,
        pk,
        pkColumns,
        SqlConstraintEnforcement.NOT_ENFORCED.symbol(SqlParserPos.ZERO),
        true,
        SqlParserPos.ZERO);
  }

  public static SqlNodeList createProperties(Map<String, String> options) {
    List<SqlNode> props =
        options.entrySet().stream()
            .map(
                option ->
                    new SqlTableOption(
                        SqlLiteral.createCharString(option.getKey(), SqlParserPos.ZERO),
                        SqlLiteral.createCharString(
                            Objects.toString(option.getValue()), SqlParserPos.ZERO),
                        SqlParserPos.ZERO))
            .collect(Collectors.toList());

    return new SqlNodeList(props, SqlParserPos.ZERO);
  }

  public static Map<String, String> propertiesToMap(SqlNodeList nodeList) {
    Map<String, String> result = new HashMap<>();
    for (SqlNode node : nodeList) {
      var option = (SqlTableOption) node;
      var keyLiteral = (SqlLiteral) option.getKey();
      var valueLiteral = (SqlLiteral) option.getValue();
      result.put(keyLiteral.toValue(), valueLiteral.toValue());
    }
    return result;
  }

  public static SqlNodeList createPartitionKeys(List<String> partitionKeys) {
    List<SqlIdentifier> keys =
        partitionKeys.stream().map(FlinkSqlNodeFactory::identifier).collect(Collectors.toList());

    return new SqlNodeList(keys, SqlParserPos.ZERO);
  }

  // New method to replace FlinkSqlTableBuilder
  public static SqlCreateTable createTable(
      String tableName,
      RelDataType relDataType,
      Optional<List<String>> partitionKeys,
      long watermarkMillis,
      Optional<String> timestampColumn,
      Map<String, MetadataEntry> metadataConfig,
      List<String> primaryKeyConstraint,
      Map<String, String> connectorProperties,
      MetadataExpressionParser expressionParser) {

    var partitionKeysNode =
        partitionKeys.map(FlinkSqlNodeFactory::createPartitionKeys).orElse(SqlNodeList.EMPTY);
    var watermark =
        timestampColumn
            .filter(ts -> watermarkMillis >= 0)
            .map(ts -> createWatermark(ts, watermarkMillis))
            .orElse(null);

    return new SqlCreateTable(
        SqlParserPos.ZERO,
        FlinkSqlNodeFactory.identifier(tableName),
        createColumns(relDataType, metadataConfig, expressionParser),
        createConstraints(primaryKeyConstraint),
        createPropertiesAndRemoveDefaults(connectorProperties),
        NO_DISTRIBUTION,
        partitionKeysNode,
        watermark,
        null,
        false,
        false);
  }

  public static SqlCreateTable createTable(
      String tableName,
      RelDataType relDataType,
      Map<String, String> connectorOptions,
      boolean isTemporary) {
    return new SqlCreateTable(
        SqlParserPos.ZERO,
        FlinkSqlNodeFactory.identifier(tableName),
        createColumns(relDataType),
        Collections.emptyList(),
        FlinkSqlNodeFactory.createProperties(connectorOptions),
        NO_DISTRIBUTION,
        SqlNodeList.EMPTY,
        null,
        null,
        isTemporary,
        false);
  }

  public static SqlWatermark createWatermark(String ts, long watermarkMillis) {
    var eventTimeColumn = FlinkSqlNodeFactory.identifier(ts);
    return FlinkSqlNodeFactory.createWatermark(
        eventTimeColumn,
        FlinkSqlNodeFactory.boundedStrategy(
            eventTimeColumn, Double.toString(watermarkMillis / 1000d)));
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
              new SqlTableColumn.SqlMetadataColumn(
                  SqlParserPos.ZERO,
                  FlinkSqlNodeFactory.identifier(columnName),
                  null,
                  SqlDataTypeSpecBuilder.convertTypeToFlinkSpec(column.getType()),
                  metadataFnc,
                  isVirtual.orElse(false));
        }
      } else {
        node =
            new SqlTableColumn.SqlRegularColumn(
                SqlParserPos.ZERO,
                FlinkSqlNodeFactory.identifier(columnName),
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
        SqlParserPos.ZERO, FlinkSqlNodeFactory.identifier(columnName), null, call);
  }

  public static SqlCall getCallWithNoArgs(SqlOperator operator) {
    return new SqlBasicCall(operator, List.of(), SqlParserPos.ZERO);
  }

  private static List<SqlTableConstraint> createConstraints(List<String> primaryKey) {
    if (primaryKey.isEmpty()) {
      return Collections.emptyList();
    }
    var pkConstraint = FlinkSqlNodeFactory.createPrimaryKeyConstraint(primaryKey);
    return Collections.singletonList(pkConstraint);
  }

  private static SqlNodeList createPropertiesAndRemoveDefaults(
      Map<String, String> connectorProperties) {
    Map<String, String> options = new TreeMap<>(connectorProperties);
    options.remove("version");
    options.remove("type");
    if (options.isEmpty()) {
      return SqlNodeList.EMPTY;
    }
    return FlinkSqlNodeFactory.createProperties(options);
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
