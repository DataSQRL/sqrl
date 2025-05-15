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
package com.datasqrl.plan.rules;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalTableModify;

/**
 * A {@link RelShuttle} that throws exceptions for all logical operators that cannot occur in an
 * SQRL logical plan.
 */
public interface SqrlRelShuttle extends RelShuttle {

  /*
  ====== Rel Nodes with default treatment =====
   */

  @Override
  default RelNode visit(LogicalIntersect logicalIntersect) {
    return visit((RelNode) logicalIntersect);
  }

  @Override
  default RelNode visit(LogicalMinus logicalMinus) {
    return visit((RelNode) logicalMinus);
  }

  @Override
  default RelNode visit(LogicalCalc logicalCalc) {
    return visit((RelNode) logicalCalc);
  }

  @Override
  default RelNode visit(LogicalExchange logicalExchange) {
    return visit((RelNode) logicalExchange);
  }

  @Override
  default RelNode visit(LogicalMatch logicalMatch) {
    return visit((RelNode) logicalMatch);
  }

  /*
  ====== Rel Nodes that do not occur in SQRL =====
  */

  @Override
  default RelNode visit(LogicalTableModify logicalTableModify) {
    throw new UnsupportedOperationException("Not yet supported.");
  }
}
