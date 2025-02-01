package com.datasqrl.calcite.function;

import com.datasqrl.calcite.Dialect;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

/**
 * Generates a rule for a given dialect to rewrite a function with structural changes to the logical plan.
 * These are then retrieved
 * and executed by {@link com.datasqrl.calcite.DialectCallConverter}. Make sure you annotate your
 * implementation with @AutoService for it to be discovered.
 *
 * Simpler transformations that only require changing the function name and/or arguments should
 * be implemented via {@link com.datasqrl.function.translations.SqlTranslation}.
 */
public interface OperatorRuleTransform {
  /**
   * Generates rules for transforming a function into another dialect. Note: the 'operator'
   * may not be the same operator your 'this' since it may undergo delegation so it is passed
   * as a parameter.
   */
  List<RelRule> transform(Dialect dialect, SqlOperator operator /* todo engine capabilities*/);

  /**
   *
   * @return The name of the operator that is being rewritten
   */
  String getRuleOperatorName();
}
