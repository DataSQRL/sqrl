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
package com.datasqrl.planner.analyzer;

import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.rules.RelHolder;
import com.datasqrl.plan.util.PrimaryKeyMap;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;

/**
 * Intermediate analysis used by the {@link SQRLLogicalPlanAnalyzer} to keep track of information as
 * it processes the relational operator tree of {@link RelNode}s.
 *
 * @see TableAnalysis for more information
 */
@Getter
@Builder(toBuilder = true)
@AllArgsConstructor
public class RelNodeAnalysis implements RelHolder, AbstractAnalysis {

  @NonNull RelNode relNode;
  @NonNull @Builder.Default TableType type = TableType.RELATION;
  @NonNull @Builder.Default PrimaryKeyMap primaryKey = PrimaryKeyMap.UNDEFINED;
  @Builder.Default Optional<TableAnalysis> streamRoot = Optional.empty();
  @Builder.Default boolean hasNowFilter = false;
}
