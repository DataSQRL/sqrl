package com.datasqrl.calcite.function;

import com.datasqrl.calcite.Dialect;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

public interface RuleTransform {
  List<RelRule> transform(Dialect dialect, SqlOperator operator /* todo engine capabilities*/);
}
