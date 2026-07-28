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
package com.datasqrl.engine.database.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.hint.PlannerHints;
import com.datasqrl.planner.parser.ParsedObject;
import com.datasqrl.planner.parser.SqrlComments;
import com.datasqrl.planner.parser.SqrlHint;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.datasqrl.planner.util.Documented;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AbstractJdbcStatementFactoryTest {

  @Mock private TableAnalysis tableAnalysis;

  @Mock private RelDataType datatype;

  @Mock private FlinkTableBuilder tableBuilder;

  private final PostgresStatementFactory factory = new PostgresStatementFactory();

  private CreateTableJdbcStatement createTable(PlannerHints hints) {
    when(tableAnalysis.getHints()).thenReturn(hints);
    when(tableAnalysis.getDocumentation()).thenReturn(Documented.EMPTY);
    when(datatype.getFieldList()).thenReturn(List.of());
    when(tableBuilder.getPrimaryKey()).thenReturn(Optional.empty());

    var createTable = new JdbcEngineCreateTable("my_table", tableBuilder, datatype, tableAnalysis);
    return (CreateTableJdbcStatement) factory.createTable(createTable);
  }

  private static PlannerHints hints(SqrlHint... sqrlHints) {
    var parsedHints =
        List.of(sqrlHints).stream()
            .map(hint -> new ParsedObject<>(hint, FileLocation.START))
            .toList();
    var comments = new SqrlComments(List.of(), parsedHints);
    return PlannerHints.from(comments, Optional.empty(), ErrorCollector.root());
  }

  @Test
  void givenTtlHint_whenCreateTable_thenTtlPopulated() {
    var stmt = createTable(hints(new SqrlHint("ttl", List.of("30 days"))));

    assertThat(stmt.getTtl()).isEqualTo(Duration.ofDays(30));
    // no partition_key hint, so the table is not range-partitioned
    assertThat(stmt.getPartitionInterval()).isNull();
  }

  @Test
  void givenNoTtlHint_whenCreateTable_thenZeroTtlAndNullInterval() {
    var stmt = createTable(PlannerHints.EMPTY);

    assertThat(stmt.getTtl()).isEqualTo(Duration.ZERO);
    assertThat(stmt.getPartitionInterval()).isNull();
  }
}
