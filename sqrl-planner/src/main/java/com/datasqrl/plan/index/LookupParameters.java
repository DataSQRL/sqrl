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
package com.datasqrl.plan.index;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;

/**
 * Creates the parameters that stand for a value of another table that a query looks up rows by,
 * like a column of the left table in a join condition, or a correlated column of the outer query in
 * a subquery. Index selection treats them like the parameters of the query.
 *
 * <p>An optional lookup parameter stands for a value that the database binds only when it reads the
 * other table first. In a semi join, for example, the database can also read this table first and
 * look up the other one, so {@link FilterAnalyzer} also summarizes the access without the value.
 */
final class LookupParameters {

  // Beyond the indexes of the parameters of a table function
  private static final int FIRST_INDEX = 10_000;
  private static final int FIRST_OPTIONAL_INDEX = 1_000_000;

  private int nextIndex = FIRST_INDEX;
  private int nextOptionalIndex = FIRST_OPTIONAL_INDEX;

  RexDynamicParam lookup(RelDataType type) {
    return new RexDynamicParam(type, nextIndex++);
  }

  RexDynamicParam optionalLookup(RelDataType type) {
    return new RexDynamicParam(type, nextOptionalIndex++);
  }

  static boolean isOptionalLookup(RexNode node) {
    return node instanceof RexDynamicParam param && param.getIndex() >= FIRST_OPTIONAL_INDEX;
  }
}
