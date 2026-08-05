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
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.plan.global.IndexType;
import com.datasqrl.sql.SqlDDLStatement;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.calcite.sql.SqlDialect;

@Value
public class CreateIndexDDL implements SqlDDLStatement {

  String indexName;
  String tableName;
  List<String> columns;
  IndexType type;
  DdlIdentifierQuoter identifierQuoter;

  public CreateIndexDDL(
      String indexName,
      String tableName,
      List<String> columns,
      IndexType type,
      SqlDialect dialect) {
    this.indexName = indexName;
    this.tableName = tableName;
    this.columns = columns;
    this.type = type;
    this.identifierQuoter = new DdlIdentifierQuoter(dialect);
  }

  @Override
  public String getSql() {
    String indexType, columnExpression;
    switch (type) {
      case TEXT:
        columnExpression =
            "to_tsvector('english', %s )"
                .formatted(
                    identifierQuoter.quoteAll(columns).stream()
                        .map("coalesce(%s, '')"::formatted)
                        .collect(Collectors.joining(" || ' ' || ")));
        indexType = "GIN";
        break;
      case VECTOR_COSINE:
      case VECTOR_EUCLID:
        Preconditions.checkArgument(columns.size() == 1);
        String indexModifier;
        indexModifier =
            switch (type) {
              case VECTOR_COSINE -> "vector_cosine_ops";
              case VECTOR_EUCLID -> "vector_l2_ops";
              default -> throw new UnsupportedOperationException(type.toString());
            };
        columnExpression = identifierQuoter.quote(columns.get(0)) + " " + indexModifier;
        indexType = "HNSW";
        break;
      default:
        columnExpression = String.join(",", identifierQuoter.quoteAll(columns));
        indexType = type.name().toLowerCase();
    }

    var createTable = "CREATE INDEX IF NOT EXISTS %s ON %s USING %s (%s)";
    var sql =
        createTable.formatted(
            identifierQuoter.quote(indexName),
            identifierQuoter.quote(tableName),
            indexType,
            columnExpression);

    return sql;
  }
}
