package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.google.auto.service.AutoService;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;

@AutoService(SqlConverter.class)
public class PostgresSqlConverter implements SqlConverter {

  @Override
  public SqlNodes convert(RelNode relNode) {
    RelToSqlConverter converter = new RelToSqlConverter(ExtendedPostgresSqlDialect.DEFAULT);
    return () -> converter.visitRoot(relNode).asStatement();
  }

  @Override
  public Dialect getDialect() {
    return Dialect.POSTGRES;
  }
}
