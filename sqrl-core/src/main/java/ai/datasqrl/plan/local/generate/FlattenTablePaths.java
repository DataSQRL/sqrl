package ai.datasqrl.plan.local.generate;

import ai.datasqrl.plan.calcite.util.SqlNodeUtil;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.AbsoluteResolvedTable;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.RelativeResolvedTable;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.Resolved;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.SingleTable;
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
    SqlNode result = node.accept(this);;
    Preconditions.checkState(pullupStack.isEmpty());
    return result;
  }

  public static void addJoinCondition(SqlJoin join, SqlNode condition) {
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
      case JOIN:
        SqlJoin join = (SqlJoin)super.visit(call);
        if (!pullupStack.isEmpty()) {
          SqlNode toAppend = pullupStack.pop();
          addJoinCondition(join, toAppend);
        }
        return join;
      case AS:
        //When we rewrite paths, we turn a left tree into a bushy tree and we'll need to add the
        // alias to the last table. So If we do that, we need to remove this alias or calcite
        // will end up wrapping it in a subquery. This is for aesthetics so remove it if it causes
        // problems.
        if (analysis.getTableIdentifiers().get(call.getOperandList().get(0)) != null) {
          return call.getOperandList().get(0).accept(this);
        }


    }

    return super.visit(call);
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (analysis.getTableIdentifiers().get(id) != null) {
      return expandTable(id);
    }
    return super.visit(id);
  }

  Stack<SqlNode> pullupStack = new Stack<>();
  @Value
  class ExpandedTable {
    SqlNode table;
    Optional<SqlNode> pullupCondition;
  }

  int idx = 0;

  private SqlNode expandTable(SqlIdentifier id) {
    String finalAlias = analysis.tableAlias.get(id);
    List<String> suffix;

    SqlIdentifier first;
    Resolved resolve = analysis.tableIdentifiers.get(id);
    if (resolve instanceof SingleTable) {
      suffix = List.of();
      first = new SqlIdentifier(List.of(
          id.names.get(0)
      ), SqlParserPos.ZERO);
    } else if (resolve instanceof AbsoluteResolvedTable) {
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
      //Create condition. It will be:
      // first alias's pk = first expanded table pk
      if (resolve instanceof RelativeResolvedTable) {
        RelativeResolvedTable t = (RelativeResolvedTable)resolve;
        SQRLTable from = t.getFields().get(0).getFromTable();
        SQRLTable to = t.getFields().get(0).getToTable();

        SqlNode condition = createCondition(firstAlias, t.getAlias(), from, to);

//        pullupStack.push(condition);
      }
    }


    return n;
  }

  public static SqlNode createCondition(String firstAlias, String alias, SQRLTable from, SQRLTable to) {
    List<SqlNode> conditions = new ArrayList<>();
    for (int i = 0; i < from.getVt().getPrimaryKeyNames().size()
        && i < to.getVt().getPrimaryKeyNames().size(); i++) {
      String pkName = from.getVt().getPrimaryKeyNames().get(i);
      SqlCall call = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
          new SqlIdentifier(List.of(alias, pkName), SqlParserPos.ZERO),
          new SqlIdentifier(List.of(firstAlias, pkName), SqlParserPos.ZERO)
      );
      conditions.add(call);
    }
    SqlNode condition = SqlNodeUtil.and(conditions);
    return condition;
  }
}
