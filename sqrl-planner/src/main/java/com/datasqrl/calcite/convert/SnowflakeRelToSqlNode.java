package com.datasqrl.calcite.convert;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverterWithHints;
import org.apache.calcite.sql.CalciteFixes;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import com.google.auto.service.AutoService;

@AutoService(RelToSqlNode.class)
public class SnowflakeRelToSqlNode implements RelToSqlNode {

  @Override
  public SqlNodes convert(RelNode relNode) {
    var node = new RelToSqlConverterWithHints(ExtendedSnowflakeSqlDialect.DEFAULT)
        .visitRoot(relNode)
        .asStatement();
    CalciteFixes.appendSelectLists(node);
    return ()->node;
  }

  @Override
  public Dialect getDialect() {
    return Dialect.SNOWFLAKE;
  }
}
