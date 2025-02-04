package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

/**
 * Converts a RelNode to a SqlNode for a given dialect
 */
public interface RelToSqlNode {
  SqlNodes convert(RelNode relNode);

  Dialect getDialect();

  interface SqlNodes {
    SqlNode getSqlNode();
  }
}
