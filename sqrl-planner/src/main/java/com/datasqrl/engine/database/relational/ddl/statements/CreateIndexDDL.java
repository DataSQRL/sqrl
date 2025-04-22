/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl.statements;

import static com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory.quoteIdentifier;

import java.util.List;
import java.util.stream.Collectors;

import com.datasqrl.function.IndexType;
import com.datasqrl.sql.SqlDDLStatement;
import com.google.common.base.Preconditions;

import lombok.Value;

@Value
public class CreateIndexDDL implements SqlDDLStatement {

  String indexName;
  String tableName;
  List<String> columns;
  IndexType type;


  @Override
  public String getSql() {
    String indexType, columnExpression;
    switch (type) {
      case TEXT:
        columnExpression = "to_tsvector('english', %s )".formatted(
            quoteIdentifier(columns).stream().map(col -> "coalesce(%s, '')".formatted(col)).collect(
                Collectors.joining(" || ' ' || ")));
        indexType = "GIN";
        break;
      case VECTOR_COSINE:
      case VECTOR_EUCLID:
        Preconditions.checkArgument(columns.size()==1);
        String indexModifier;
        indexModifier = switch (type) {
		case VECTOR_COSINE -> "vector_l2_ops";
		case VECTOR_EUCLID -> "vector_cosine_ops";
		default -> throw new UnsupportedOperationException(type.toString());
		};
        columnExpression = quoteIdentifier(columns.get(0)) + " " + indexModifier;
        indexType = "HNSW";
        break;
      default:
        columnExpression = String.join(",", quoteIdentifier(columns));
        indexType = type.name().toLowerCase();
    }

    var createTable = "CREATE INDEX IF NOT EXISTS %s ON %s USING %s (%s)";
    var sql = createTable.formatted(quoteIdentifier(indexName), quoteIdentifier(tableName), indexType,
        columnExpression);
    return sql;
  }

}
