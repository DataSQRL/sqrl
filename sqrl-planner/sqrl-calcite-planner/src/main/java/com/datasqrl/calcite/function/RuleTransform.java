package com.datasqrl.calcite.function;

import org.apache.calcite.plan.RelRule;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

public interface RuleTransform {
  List<RelRule> transform(SqlDialect dialect, SqlOperator operator /* todo engine capabilities*/);
}
