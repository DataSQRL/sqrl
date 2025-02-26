package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.google.auto.service.AutoService;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.RelToSqlConverterWithHints;

@AutoService(RelToSqlNode.class)
public class PostgresRelToSqlNode implements RelToSqlNode {

  @Override
  public SqlNodes convert(RelNode relNode) {
    RelToSqlConverter converter =
        new RelToSqlConverterWithHints(ExtendedPostgresSqlDialect.DEFAULT);
    return () -> converter.visitRoot(relNode).asStatement();
  }

  @Override
  public Dialect getDialect() {
    return Dialect.POSTGRES;
  }
}
