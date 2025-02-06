package com.datasqrl.calcite.convert;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

import com.datasqrl.calcite.Dialect;

//Service loader interface
public interface RelToSqlNode {
  SqlNodes convert(RelNode relNode);

  Dialect getDialect();

  interface SqlNodes {
    SqlNode getSqlNode();
  }
}
