/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.calcite.function;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.function.translation.SqlTranslation;
import java.util.List;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.sql.SqlOperator;

/**
 * Generates a rule for a given dialect to rewrite a function with structural changes to the logical
 * plan. These are then retrieved and executed by {@link
 * com.datasqrl.calcite.OperatorRuleTransformer}. Make sure you annotate your implementation
 * with @AutoService for it to be discovered.
 *
 * <p>Simpler transformations that only require changing the function name and/or arguments should
 * be implemented via {@link SqlTranslation}.
 */
public interface OperatorRuleTransform {
  /**
   * Generates rules for transforming a function into another dialect. Note: the 'operator' may not
   * be the same operator your 'this' since it may undergo delegation so it is passed as a
   * parameter.
   */
  List<RelRule> transform(SqlOperator operator /* todo engine capabilities*/);

  Dialect getDialect();

  /**
   * @return The name of the operator that is being rewritten
   */
  String getRuleOperatorName();
}
