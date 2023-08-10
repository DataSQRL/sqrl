package com.datasqrl.calcite.convert;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;

public class PostgresSqlConverter extends SqlConverter {

  @Override
  public SqlNode convert(RelNode relNode) {
    RelToSqlConverter converter = new RelToSqlConverter(PostgresqlSqlDialect.DEFAULT);
    return converter.visitRoot(relNode).asStatement();
  }

}
