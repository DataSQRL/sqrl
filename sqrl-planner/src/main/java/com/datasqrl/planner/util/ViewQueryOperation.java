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
package com.datasqrl.planner.util;

import java.util.function.Supplier;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

/** Backs a catalog view with its planned tree, handing out a fresh copy per reference. */
public class ViewQueryOperation extends PlannerQueryOperation {

  public ViewQueryOperation(RelNode calciteTree, Supplier<String> toSqlString) {
    super(calciteTree, toSqlString);
  }

  @Override
  public RelNode getCalciteTree() {
    return RelNodeCopier.copy(super.getCalciteTree());
  }
}
