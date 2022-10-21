package ai.datasqrl.plan.local.generate;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Expands select *, also needs to add an alias to tables that need one
 */
public class QualifyIdentifiers extends SqlShuttle {

  private final AnalyzeStatement analysis;

  public QualifyIdentifiers(AnalyzeStatement analysis) {
    this.analysis = analysis;
  }

  //the only time we should be here is if we're a table identifier w/ no alias
  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (analysis.mayNeedAlias.get(id) != null) {
      return SqlStdOperatorTable.AS.createCall(
          SqlParserPos.ZERO,
          id,
          new SqlIdentifier(analysis.mayNeedAlias.get(id), SqlParserPos.ZERO)
      );
    }

    return super.visit(id);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case SELECT:
        return rewriteSelect((SqlSelect) call);
      case JOIN:
        SqlJoin join = (SqlJoin) call;
        join.setRight(join.getRight().accept(this));
        join.setLeft(join.getLeft().accept(this));
        return join;
      case AS:
        //only walk table
        SqlNode aliased = call.getOperandList().get(0)
            .accept(this);
        call.setOperand(0, aliased);
        return call;
    }

    return super.visit(call);
  }

  private SqlNode rewriteSelect(SqlSelect select) {
    select.setFrom(select.getFrom().accept(this));

    List<SqlNode> expandedSelect = analysis.expandedSelect.get(select);
    SqlNodeList nodeList = new SqlNodeList(expandedSelect, select.getSelectList().getParserPosition());
    select.setSelectList((SqlNodeList) expr(nodeList));

    if (select.getWhere() != null) {
      select.setWhere(expr(select.getWhere()));
    }

    if (select.getGroup() != null) {
      select.setGroupBy(replaceGroupBy(select.getGroup()));
    }
    if (select.getHaving() != null) {
      select.setHaving(expr(select.getHaving()));
    }

    if (select.getOrderList() != null) {
      select.setOrderBy((SqlNodeList) expr(select.getOrderList()));
    }

    return select;
  }

  private SqlNodeList replaceGroupBy(SqlNodeList group) {
    List<SqlNode> full = analysis.groupByExpressions.get(group);

    return new SqlNodeList(full, group.getParserPosition());
  }

  public SqlNode expr(SqlNode node) {
    return node.accept(new QualifyExpression());
  }

  public class QualifyExpression extends SqlShuttle {

    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (analysis.getExpressions().containsKey(id)) {
        return analysis.getExpressions().get(id).getAliasedIdentifier(id);
      }
      
      return super.visit(id);
    }
  }
}
