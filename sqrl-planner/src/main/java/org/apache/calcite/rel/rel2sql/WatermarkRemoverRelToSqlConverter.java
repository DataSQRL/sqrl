package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;

public class WatermarkRemoverRelToSqlConverter extends RelToSqlConverter {

  public WatermarkRemoverRelToSqlConverter(SqlDialect dialect) {
    super(dialect);
  }

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
