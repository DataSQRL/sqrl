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

import com.datasqrl.plan.global.QueryIndexSummary;
import com.datasqrl.plan.global.QueryIndexSummary.IndexableFunctionCall;
import com.datasqrl.planner.analyzer.TableAnalysis;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

public class SqrlRelMdSelectivity extends RelMdSelectivity
    implements BuiltInMetadata.Selectivity.Handler {

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.SELECTIVITY.method, new SqrlRelMdSelectivity());

  @Override
  public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
    return super.getSelectivity(rel, mq, predicate);
  }

  public static Double getSelectivity(TableAnalysis table, QueryIndexSummary constraints) {
    // TODO: use actual selectivity statistics from table
    var selectivity = 1.0d;
    selectivity *= Math.pow(0.05, constraints.getEqualityColumns().size());
    selectivity *= Math.pow(0.5, constraints.getInequalityColumns().size());
    for (IndexableFunctionCall fcall : constraints.getFunctionCalls()) {
      selectivity *= fcall.function().estimateSelectivity();
    }
    return selectivity;
  }
}
