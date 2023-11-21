package com.datasqrl.calcite.function;

import com.datasqrl.calcite.Dialect;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

public interface RuleTransform {
  /**
   * Generates rules for transforming a function into another dialect. Note: the 'operator'
   * may not be the same operator your 'this' since it may undergo delegation so it is passed
   * as a parameter.
   */
  List<RelRule> transform(Dialect dialect, SqlOperator operator /* todo engine capabilities*/);

  String getRuleOperatorName();
}
