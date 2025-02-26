package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.google.auto.service.AutoService;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverterWithHints;
import org.apache.calcite.sql.CalciteFixes;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

@AutoService(RelToSqlNode.class)
public class CalciteRelToSqlNode implements RelToSqlNode {

  @Override
  public SqlNodes convert(RelNode relNode) {
    SqlNode node =
        new RelToSqlConverterWithHints(CalciteSqlDialect.DEFAULT).visitRoot(relNode).asStatement();
    CalciteFixes.appendSelectLists(node);
    return () -> node;
  }

  @Override
  public Dialect getDialect() {
    return Dialect.CALCITE;
  }
}
