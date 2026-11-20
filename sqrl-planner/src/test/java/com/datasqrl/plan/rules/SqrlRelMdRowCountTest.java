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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datasqrl.plan.index.QueryIndexSummary;
import com.datasqrl.plan.table.TableStatistic;
import com.datasqrl.planner.analyzer.TableAnalysis;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.junit.jupiter.api.Test;

class SqrlRelMdRowCountTest {

  private static final QueryIndexSummary EQUALITY_ON_ONE_COLUMN =
      new QueryIndexSummary(null, Set.of(0), Set.of(), Set.of(), 1.0);

  @Test
  void givenLargeTable_whenGetRowCount_thenRowCountTimesSelectivity() {
    var table = tableWith(TableStatistic.fromHint(1e6));

    assertThat(SqrlRelMdRowCount.getRowCount(table, EQUALITY_ON_ONE_COLUMN)).isEqualTo(1e6 * 0.05);
  }

  @Test
  void givenTableOfFewRows_whenGetRowCount_thenAtLeastOneRow() {
    var table = tableWith(TableStatistic.fromEstimate(3));

    assertThat(SqrlRelMdRowCount.getRowCount(table, EQUALITY_ON_ONE_COLUMN)).isEqualTo(1.0);
  }

  private static TableAnalysis tableWith(TableStatistic tableStatistic) {
    var relNode = mock(RelNode.class);
    return TableAnalysis.builder()
        .objectIdentifier(ObjectIdentifier.of("datasqrl", "public", "Customers"))
        .collapsedRelnode(relNode)
        .originalRelnode(relNode)
        .tableStatistic(tableStatistic)
        .build();
  }
}
