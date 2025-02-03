package org.apache.calcite.rel.rel2sql;

import com.datasqrl.v2.TableAnalysisLookup;
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

//  public SqlImplementor.Result visitInput(RelNode parent, int i, boolean anon, boolean ignoreClauses, Set<Clause> expectedClauses) {
//    RelNode input = parent.getInput(i);
//    Optional<TableAnalysis> tableAnalysis = tableLookup.lookupTable(input);
//    if (tableAnalysis.isPresent()) {
//      SqlIdentifier identifier = FlinkSqlNodeFactory.identifier(tableAnalysis.get().getIdentifier());
//      SqlNode select = new SqlSelect(SqlParserPos.ZERO, (SqlNodeList)null, SqlNodeList.SINGLETON_STAR, identifier, (SqlNode)null, (SqlNodeList)null, (SqlNode)null, (SqlNodeList)null, (SqlNodeList)null, (SqlNode)null, (SqlNode)null, SqlNodeList.EMPTY);
//      return this.result(select, ImmutableList.of(Clause.SELECT), input, (Map)null);
//    } else {
//      return super.visitInput(parent, i, anon, ignoreClauses, expectedClauses);
//    }
//  }

  public Result visit(LogicalWatermarkAssigner e) {
    Result rIn = dispatch(e.getInput());
    if (rIn.node instanceof SqlSelect) {
      //We don't want to process anything below the watermark assigner since those are table internals
      SqlSelect select = (SqlSelect) rIn.node;
      select.setSelectList(SqlNodeList.of(SqlIdentifier.STAR));
    }
    return rIn;
  }

}
