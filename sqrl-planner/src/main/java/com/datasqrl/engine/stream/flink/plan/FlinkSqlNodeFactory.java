package com.datasqrl.engine.stream.flink.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlConstraintEnforcement;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.constraint.SqlUniqueSpec;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

import com.datasqrl.calcite.schema.sql.SqlDataTypeSpecBuilder;
import com.datasqrl.config.TableConfig.MetadataEntry;
import com.datasqrl.sql.SqlCallRewriter;

public class FlinkSqlNodeFactory {

  public static SqlIdentifier identifier(String str) {
    return new SqlIdentifier(str, SqlParserPos.ZERO);
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
        null
    );
  }

  public static RichSqlInsert createInsert(SqlNode source, String target) {
    return new RichSqlInsert(
        SqlParserPos.ZERO,
        SqlNodeList.EMPTY,
        SqlNodeList.EMPTY,
        identifier(target),
        source,
        null,
        null
    );
  }

  public static SqlCreateFunction createFunction(String name, String clazz) {
    return new SqlCreateFunction(
        SqlParserPos.ZERO,
        identifier(name),
        SqlLiteral.createCharString(clazz, SqlParserPos.ZERO),
        "JAVA",
        true,
        true,
        false,
        new SqlNodeList(SqlParserPos.ZERO)
    );
  }

  public static SqlWatermark createWatermark(SqlIdentifier eventTimeColumn, SqlNode watermarkStrategy) {
    return new SqlWatermark(
        SqlParserPos.ZERO,
        eventTimeColumn,
        watermarkStrategy
    );
  }

  public static SqlNode boundedStrategy(SqlNode watermark, String delay) {
    return new SqlBasicCall(
        SqlStdOperatorTable.MINUS,
        new SqlNode[]{
            watermark,
            SqlLiteral.createInterval(
                1,
                delay,
                new SqlIntervalQualifier(
                    TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO
                ),
                SqlParserPos.ZERO
            )
        },
        SqlParserPos.ZERO
    );
  }

  public static SqlTableConstraint createPrimaryKeyConstraint(List<String> primaryKey) {
    var pk = SqlUniqueSpec.PRIMARY_KEY.symbol(SqlParserPos.ZERO);
    var pkColumns = new SqlNodeList(
        primaryKey.stream()
            .map(FlinkSqlNodeFactory::identifier)
            .collect(Collectors.toList()),
        SqlParserPos.ZERO
    );

    return new SqlTableConstraint(
        null,
        pk,
        pkColumns,
        SqlConstraintEnforcement.NOT_ENFORCED.symbol(SqlParserPos.ZERO),
        true,
        SqlParserPos.ZERO
    );
  }

  public static SqlNodeList createProperties(Map<String, Object> options) {
    List<SqlNode> props = options.entrySet().stream()
        .map(option -> new SqlTableOption(
            SqlLiteral.createCharString(option.getKey(), SqlParserPos.ZERO),
            SqlLiteral.createCharString(option.getValue().toString(), SqlParserPos.ZERO),
            SqlParserPos.ZERO
        ))
        .collect(Collectors.toList());

    return new SqlNodeList(props, SqlParserPos.ZERO);
  }

  public static SqlNodeList createPartitionKeys(List<String> partitionKeys) {
    List<SqlIdentifier> keys = partitionKeys.stream()
        .map(FlinkSqlNodeFactory::identifier)
        .collect(Collectors.toList());

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
      Map<String, Object> connectorProperties,
      MetadataExpressionParser expressionParser) {

    var partitionKeysNode = partitionKeys
        .map(FlinkSqlNodeFactory::createPartitionKeys)
        .orElse(SqlNodeList.EMPTY);
    var watermark = timestampColumn
        .filter(ts -> watermarkMillis >= 0)
        .map(ts -> createWatermark(ts, watermarkMillis))
        .orElse(null);

    return new SqlCreateTable(
        SqlParserPos.ZERO,
        FlinkSqlNodeFactory.identifier(tableName),
        createColumns(relDataType, metadataConfig, expressionParser),
        createConstraints(primaryKeyConstraint),
        createPropertiesAndRemoveDefaults(connectorProperties),
        partitionKeysNode,
        watermark,
        null,
        true,
        false
    );
  }

  private static SqlWatermark createWatermark(String ts, long watermarkMillis) {
    var eventTimeColumn = FlinkSqlNodeFactory.identifier(ts);
    return FlinkSqlNodeFactory.createWatermark(
        eventTimeColumn,
        FlinkSqlNodeFactory.boundedStrategy(eventTimeColumn, Double.toString(watermarkMillis / 1000d))
    );
  }

  private static SqlNodeList createColumns(RelDataType relDataType, Map<String, MetadataEntry> metadataConfig,
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
        var attribute = metadataEntry.getAttribute();
        var isVirtual = metadataEntry.getVirtual();
        SqlNode metadataFnc;

        if (attribute.isEmpty()) {
          metadataFnc = SqlLiteral.createCharString(metadataEntry.getType().get(), SqlParserPos.ZERO);
        } else {
          metadataFnc = expressionParser.parseExpression(attribute.get());
          if (metadataFnc instanceof SqlIdentifier) {
            metadataFnc = SqlLiteral.createCharString(attribute.get(), SqlParserPos.ZERO);
          } else {
            new SqlCallRewriter().performCallRewrite((SqlCall) metadataFnc);
          }
        }

        if (metadataFnc instanceof SqlCall) {
          node = new SqlTableColumn.SqlComputedColumn(
              SqlParserPos.ZERO,
              FlinkSqlNodeFactory.identifier(columnName),
              null,
              metadataFnc
          );
        } else {
          node = new SqlTableColumn.SqlMetadataColumn(
              SqlParserPos.ZERO,
              FlinkSqlNodeFactory.identifier(columnName),
              null,
              SqlDataTypeSpecBuilder.convertTypeToFlinkSpec(column.getType()),
              metadataFnc,
              isVirtual.orElse(false)
          );
        }
      } else {
        node = new SqlTableColumn.SqlRegularColumn(
            SqlParserPos.ZERO,
            FlinkSqlNodeFactory.identifier(columnName),
            null,
            SqlDataTypeSpecBuilder.convertTypeToFlinkSpec(column.getType()),
            null
        );
      }
      nodes.add(node);
    }

    return new SqlNodeList(nodes, SqlParserPos.ZERO);
  }


  private static List<SqlTableConstraint> createConstraints(List<String> primaryKey) {
    if (primaryKey.isEmpty()) {
      return Collections.emptyList();
    }
    var pkConstraint = FlinkSqlNodeFactory.createPrimaryKeyConstraint(primaryKey);
    return Collections.singletonList(pkConstraint);
  }

  private static SqlNodeList createPropertiesAndRemoveDefaults(Map<String, Object> connectorProperties) {
    Map<String, Object> options = new HashMap<>(connectorProperties);
    options.remove("version");
    options.remove("type");
    if (options.isEmpty()) {
      return SqlNodeList.EMPTY;
    }
    return FlinkSqlNodeFactory.createProperties(options);
  }

  // Interface for parsing expressions
  public interface MetadataExpressionParser {
    SqlNode parseExpression(String expression);
  }
}
