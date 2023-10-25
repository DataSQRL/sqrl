package com.datasqrl.engine.stream.flink.sql.calcite;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

public class FlinkDialect extends SqlDialect {
  public static final SqlDialect.Context DEFAULT_CONTEXT;
  public static final SqlDialect DEFAULT;

  public FlinkDialect(SqlDialect.Context context) {
    super(context);
  }

  static {
    DEFAULT_CONTEXT = SqlDialect.EMPTY_CONTEXT
        .withConformance(FlinkSqlConformance.DEFAULT)
        .withDatabaseProduct(DatabaseProduct.UNKNOWN)
        .withLiteralQuoteString("`")
        .withLiteralEscapedQuoteString("`")
        .withIdentifierQuoteString("`");
    DEFAULT = new FlinkDialect(DEFAULT_CONTEXT);
  }

  @Override
  public boolean supportsImplicitTypeCoercion(RexCall call) {
    return false;
  }
}