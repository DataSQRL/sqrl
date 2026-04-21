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

import com.datasqrl.planner.TableAnalysisLookup;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class SqrlRelMetadataQuery extends RelMetadataQuery {

  BuiltInMetadata.RowCount.Handler rowCountHandler;
  BuiltInMetadata.Selectivity.Handler selectivityHandler;

  public SqrlRelMetadataQuery() {
    this(null);
  }

  public SqrlRelMetadataQuery(@Nullable TableAnalysisLookup tableLookup) {
    super();
    this.rowCountHandler = new SqrlRelMdRowCount(tableLookup);
    this.selectivityHandler = new SqrlRelMdSelectivity();
  }

  @Override
  public Double getRowCount(RelNode rel) {
    for (; ; ) {
      try {
        Double result = rowCountHandler.getRowCount(rel, this);
        return RelMdUtil.validateResult(result);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        rowCountHandler = revise(e.relClass, BuiltInMetadata.RowCount.DEF);
      }
    }
  }

  @Override
  public Double getSelectivity(RelNode rel, RexNode predicate) {
    for (; ; ) {
      try {
        Double result = selectivityHandler.getSelectivity(rel, this, predicate);
        return RelMdUtil.validatePercentage(result);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        selectivityHandler = revise(e.relClass, BuiltInMetadata.Selectivity.DEF);
      }
    }
  }
}
