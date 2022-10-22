package ai.datasqrl.plan.local.generate;

import ai.datasqrl.plan.calcite.util.SqlNodeUtil;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.AbsoluteResolvedTable;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.RelativeResolvedTable;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.Resolved;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.SingleTable;
import ai.datasqrl.schema.Relationship;
import com.google.common.base.Preconditions;
import java.util.Stack;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

public class ReplaceWithVirtualTable extends SqlShuttle {

  private final AnalyzeStatement analysis;

  Stack<SqlNode> pullup = new Stack<>();

  public ReplaceWithVirtualTable(AnalyzeStatement analysis) {

    this.analysis = analysis;
  }

  public SqlNode accept(SqlNode node) {

    SqlNode result = node.accept(this);

    Preconditions.checkState(pullup.isEmpty());
    return result;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case SELECT:
        SqlSelect select = (SqlSelect) call;
        while (!pullup.isEmpty()) {
          SqlNode condition = pullup.pop();

          appendToSelect(select, condition);
        }
        return select;
      case JOIN:
        SqlJoin join = (SqlJoin) call;
        while (!pullup.isEmpty()) {
          SqlNode condition = pullup.pop();
          FlattenTablePaths.addJoinCondition(join, condition);
        }

        return super.visit(call);
    }

    return super.visit(call);
  }

  private void appendToSelect(SqlSelect select, SqlNode condition) {
    SqlNode where = select.getWhere();
    if (where == null) {
      select.setWhere(condition);
    } else {
      select.setWhere(SqlNodeUtil.and(select.getWhere(), condition));
    }

  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (analysis.getTableIdentifiers().get(id) == null) {
      return super.visit(id);
    }

    Resolved resolved = analysis.getTableIdentifiers().get(id);
    if (resolved instanceof SingleTable) {
      SingleTable singleTable = (SingleTable) resolved;
      return new SqlIdentifier(singleTable.getToTable().getVt().getNameId(),
          id.getParserPosition());
    } else if (resolved instanceof AbsoluteResolvedTable) {
      throw new RuntimeException("unexpected type");
    } else if (resolved instanceof RelativeResolvedTable) {
      RelativeResolvedTable resolveRel = (RelativeResolvedTable) resolved;
      Preconditions.checkState(resolveRel.getFields().size() == 1);
      Relationship relationship = resolveRel.getFields().get(0);
      if (relationship.getNode() != null) {
        //todo: join expansion
        return super.visit(id);
      }
      //create join condition to pull up
//      FlattenTablePaths.createCondition()
      String alias = analysis.tableAlias.get(id);
      SqlNode condition = FlattenTablePaths.createCondition(alias, resolveRel.getAlias(),
          relationship.getFromTable(),
          relationship.getToTable()
      );
      pullup.push(condition);

      return new SqlIdentifier(relationship.getToTable().getVt().getNameId(),
          id.getParserPosition());
    }

    return super.visit(id);
  }

}
