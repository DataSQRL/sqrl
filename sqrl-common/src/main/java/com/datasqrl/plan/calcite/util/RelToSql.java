/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.util;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;

import java.util.function.UnaryOperator;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

public class RelToSql {

  public static final UnaryOperator<SqlWriterConfig> transform = c ->
      c.withAlwaysUseParentheses(false)
          .withSelectListItemsOnSeparateLines(false)
          .withUpdateSetListNewline(false)
          .withIndentation(1)
          .withQuoteAllIdentifiers(true)
          .withDialect(PostgresqlSqlDialect.DEFAULT)
          .withSelectFolding(null);

  public static SqlNode convertToSqlNode(RelNode optimizedNode) {
    RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
    final SqlNode sqlNode = converter.visitRoot(optimizedNode).asStatement();
    return sqlNode;
  }

  public static String convertToSql(RelNode optimizedNode) {

    String sql = convertToSqlNode(optimizedNode).toSqlString(
            c -> transform.apply(c.withDialect(PostgresqlSqlDialect.DEFAULT)))
        .getSql();
    return sql;
  }

  public static String toSql(RelDataTypeField field) {
    RelDataType datatype = field.getType();
    Preconditions.checkArgument(!CalciteUtil.isNestedTable(datatype),
        "Collection column encountered");
    return toSql(field.getName(), getSQLType(datatype), datatype.isNullable());
  }

  private static String toSql(String name, String sqlType, boolean nullable) {
    StringBuilder sql = new StringBuilder();
    sql.append(name).append(" ").append(sqlType).append(" ");
    if (!nullable) {
      sql.append("NOT NULL");
    }
    return sql.toString();
  }

  private static String getSQLType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return "BOOLEAN";
      case TINYINT:
      case SMALLINT:
      case BIGINT:
      case INTEGER:
        return "BIGINT";
      case CHAR:
      case VARCHAR:
        return "VARCHAR";
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
        return "DECIMAL";
      case DATE:
        return "DATE";
      case TIME:
        return "TIME";
      case TIMESTAMP:
        return "TIMESTAMP";
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return "TIMESTAMPTZ";
      case BINARY:
      case VARBINARY:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY:
      case NULL:
      case SYMBOL:
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
  }


}
