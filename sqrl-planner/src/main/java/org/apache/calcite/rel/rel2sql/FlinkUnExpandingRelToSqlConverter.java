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
package org.apache.calcite.rel.rel2sql;

import com.datasqrl.planner.TableAnalysisLookup;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;

public class FlinkUnExpandingRelToSqlConverter extends RelToSqlConverter {

  final TableAnalysisLookup tableLookup;

  public FlinkUnExpandingRelToSqlConverter(SqlDialect dialect, TableAnalysisLookup tableLookup) {
    super(dialect);
    this.tableLookup = tableLookup;
  }

  //  public SqlImplementor.Result visitInput(RelNode parent, int i, boolean anon, boolean
  // ignoreClauses, Set<Clause> expectedClauses) {
  //    RelNode input = parent.getInput(i);
  //    Optional<TableAnalysis> tableAnalysis = tableLookup.lookupTable(input);
  //    if (tableAnalysis.isPresent()) {
  //      SqlIdentifier identifier =
  // FlinkSqlNodeFactory.identifier(tableAnalysis.get().getIdentifier());
  //      SqlNode select = new SqlSelect(SqlParserPos.ZERO, (SqlNodeList)null,
  // SqlNodeList.SINGLETON_STAR, identifier, (SqlNode)null, (SqlNodeList)null, (SqlNode)null,
  // (SqlNodeList)null, (SqlNodeList)null, (SqlNode)null, (SqlNode)null, SqlNodeList.EMPTY);
  //      return this.result(select, ImmutableList.of(Clause.SELECT), input, (Map)null);
  //    } else {
  //      return super.visitInput(parent, i, anon, ignoreClauses, expectedClauses);
  //    }
  //  }

  public Result visit(LogicalWatermarkAssigner e) {
    var rIn = dispatch(e.getInput());
    if (rIn.node instanceof SqlSelect select) {
      select.setSelectList(SqlNodeList.of(SqlIdentifier.STAR));
    }
    return rIn;
  }
}
