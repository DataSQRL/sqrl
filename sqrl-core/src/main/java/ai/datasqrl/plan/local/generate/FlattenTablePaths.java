package ai.datasqrl.plan.local.generate;

import ai.datasqrl.plan.calcite.util.SqlNodeUtil;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.AbsoluteResolvedTable;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.RelativeResolvedTable;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.Resolved;
import ai.datasqrl.schema.SQRLTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import lombok.Value;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

public class FlattenTablePaths extends SqlShuttle {
  private final AnalyzeStatement analysis;

  public FlattenTablePaths(AnalyzeStatement analysis) {
    this.analysis = analysis;
  }

  public SqlNode accept(SqlNode node) {
    if (node instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) node;
      SqlNode from = select.getFrom().accept(this);
      while (!pullupStack.isEmpty()) {
        SqlNode condition = pullupStack.pop();
        if (from instanceof SqlJoin) {
          SqlJoin join = (SqlJoin) from;
          addJoinCondition(join, condition);

        } else {
          if (select.getWhere() ==null) {
            select.setWhere(condition);
          } else {
            select.setWhere(SqlNodeUtil.and(condition));
          }
        }
      }

      select.setFrom(from);
      Preconditions.checkState(pullupStack.isEmpty());
      return node;
    }
    //todo UNION

//    throw new RuntimeException("Union, etc, todo");
    return node;
  }

  private void addJoinCondition(SqlJoin join, SqlNode condition) {
    if (join.getCondition() == null) {
      join.setOperand(5, condition);
    } else {
      join.setOperand(5, SqlNodeUtil.and(join.getCondition(), condition));
    }
    join.setOperand(4, JoinConditionType.ON.symbol(SqlParserPos.ZERO));
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case SELECT:
        SqlSelect select = (SqlSelect) call;
        //walk only from
        select.setFrom(select.getFrom().accept(this));
        return select;
      case JOIN:
        SqlJoin join = (SqlJoin) super.visit(call);
        if (!pullupStack.isEmpty()) {
          SqlNode toAppend = pullupStack.pop();
          addJoinCondition(join, toAppend);
        }
        return join;
      case AS:

        if (call.getOperandList().get(0) instanceof SqlIdentifier &&
            ((SqlIdentifier) call.getOperandList().get(0)).names.size() > 1
        ) {
          ExpandedTable table = expandTable((SqlIdentifier) call.getOperandList().get(0),
              ((SqlIdentifier) call.getOperandList().get(1)).names.get(0));
          table.pullupCondition.map(c->pullupStack.push(c));
          return table.table;
        }
    }

    return super.visit(call);
  }

  Stack<SqlNode> pullupStack = new Stack<>();
  @Value
  class ExpandedTable {
    SqlNode table;
    Optional<SqlNode> pullupCondition;
  }

  int idx = 0;

  private ExpandedTable expandTable(SqlIdentifier id, String finalAlias) {
    List<String> suffix;

    SqlIdentifier first;
    Resolved resolve = analysis.tableIdentifiers.get(id);
    if (resolve instanceof AbsoluteResolvedTable) {
      suffix = id.names.subList(1, id.names.size());
      first = new SqlIdentifier(List.of(
          id.names.get(0)
      ), SqlParserPos.ZERO);
    } else if (resolve instanceof RelativeResolvedTable){
      first = new SqlIdentifier(List.of( id.names.get(0),
          id.names.get(1)
      ), SqlParserPos.ZERO);
      if ( id.names.size() > 2) {
        suffix =  id.names.subList(2, id.names.size());
      } else {
        suffix = List.of();
      }

    } else {
      throw new RuntimeException("");
    }

    String firstAlias = "_g" + (++idx);
    SqlNode n =
        SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
            first,
            new SqlIdentifier(suffix.size() > 0 ? firstAlias: finalAlias,
                SqlParserPos.ZERO));
    if (suffix.size() >= 1) {
      String currentAlias = "_g" + (idx);

      for (int i = 0; i < suffix.size(); i++) {
        String nextAlias = i == suffix.size() - 1 ? finalAlias : "_g" + (++idx);
        n = new SqlJoin(
            SqlParserPos.ZERO,
            n,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            JoinType.DEFAULT.symbol(SqlParserPos.ZERO),
            SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
                new SqlIdentifier(List.of(currentAlias, suffix.get(i)), SqlParserPos.ZERO),
                new SqlIdentifier(nextAlias, SqlParserPos.ZERO)),
            JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
            null
        );
        currentAlias = nextAlias;
      }
      System.out.println();

      //Create condition. It will be:
      // first alias's pk = first expanded table pk
      if (resolve instanceof RelativeResolvedTable) {
        RelativeResolvedTable t = (RelativeResolvedTable)resolve;
        SQRLTable from = t.getFields().get(0).getFromTable();
        SQRLTable to = t.getFields().get(0).getToTable();

        List<SqlNode> conditions = new ArrayList<>();
        for (int i = 0; i < from.getVt().getPrimaryKeyNames().size()
            && i < to.getVt().getPrimaryKeyNames().size(); i++) {
          String pkName = from.getVt().getPrimaryKeyNames().get(i);
          SqlCall call = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
              new SqlIdentifier(List.of(t.getAlias(), pkName), SqlParserPos.ZERO),
              new SqlIdentifier(List.of(firstAlias, pkName), SqlParserPos.ZERO)
              );
          conditions.add(call);
        }
        SqlNode condition = SqlNodeUtil.and(conditions);
        return new ExpandedTable(n, Optional.of(condition));
      }


      return new ExpandedTable(n, Optional.empty());
    }

    return new ExpandedTable(n, Optional.empty());
  }
}
