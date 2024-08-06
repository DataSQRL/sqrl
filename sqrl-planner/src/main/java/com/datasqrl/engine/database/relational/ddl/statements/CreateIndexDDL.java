/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl.statements;

import static com.datasqrl.engine.database.relational.ddl.PostgresDDLFactory.quoteIdentifier;

import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.function.IndexType;
import com.google.common.base.Preconditions;
import java.util.stream.Collectors;
import lombok.Value;

import java.util.List;

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
        columnExpression = String.format("to_tsvector('english', %s )",
            quoteIdentifier(columns).stream().map(col -> String.format("coalesce(%s, '')", col)).collect(
                Collectors.joining(" || ' ' || ")));
        indexType = "GIN";
        break;
      case VEC_COSINE:
      case VEC_EUCLID:
        Preconditions.checkArgument(columns.size()==1);
        String indexModifier;
        switch (type) {
          case VEC_COSINE:
            indexModifier = "vector_l2_ops";
            break;
          case VEC_EUCLID:
            indexModifier = "vector_cosine_ops";
            break;
          default:
            throw new UnsupportedOperationException(type.toString());
        }
        columnExpression = quoteIdentifier(columns.get(0)) + " " + indexModifier;
        indexType = "HNSW";
        break;
      default:
        columnExpression = String.join(",", quoteIdentifier(columns));
        indexType = type.name().toLowerCase();
    }

    String createTable = "CREATE INDEX IF NOT EXISTS %s ON %s USING %s (%s);";
    String sql = String.format(createTable, indexName, tableName, indexType,
        columnExpression);
    return sql;
  }

}
