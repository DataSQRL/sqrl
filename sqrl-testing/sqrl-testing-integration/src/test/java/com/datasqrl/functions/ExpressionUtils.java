/*
 * Copyright © 2025 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.functions;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

final class ExpressionUtils {

  static Expression buildExclusiveInterval(String col, Object start, Object end) {
    return Expressions.and(Expressions.greaterThan(col, start), Expressions.lessThan(col, end));
  }

  static Expression buildEqualsList(String col, Object[] vals) {
    if (vals == null || vals.length == 0) {
      throw new IllegalArgumentException("Partition values must not be null or empty");
    }

    Expression expr = Expressions.equal(col, vals[0]);
    for (int i = 1; i < vals.length; ++i) {
      expr = Expressions.or(expr, Expressions.equal(col, vals[i]));
    }

    return expr;
  }

  private ExpressionUtils() {
    throw new UnsupportedOperationException();
  }
}
