package ai.datasqrl.plan.local.generate;

import ai.datasqrl.plan.calcite.util.SqlNodeUtil;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.AbsoluteResolvedTable;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.RelativeResolvedTable;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.Resolved;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.SingleTable;
import ai.datasqrl.schema.Relationship;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

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
        SqlSelect select = (SqlSelect) super.visit(call);
        while (!pullup.isEmpty()) {
          SqlNode condition = pullup.pop();

          appendToSelect(select, condition);
        }
        return select;
      case JOIN:
        //Any right joins that are bushy don't need to pull up identifiers.

        SqlJoin join = (SqlJoin) super.visit(call);
        //if right is a bushy tree, we don't need to add the conditions (done in flattentablepaths)
//        if (join.getRight() instanceof SqlJoin) {
//          pullup.clear();
//        }

//        while (!pullup.isEmpty()) {
        if (!pullup.isEmpty()) {
          SqlNode condition = pullup.pop();
          FlattenTablePaths.addJoinCondition(join, condition);
        }

        return join;
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

        return super.visit(id);
//        return expandJoinDeclaration(resolveRel, id, relationship, relationship.getNode());
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

  private SqlNode expandJoinDeclaration(RelativeResolvedTable resolveRel, SqlIdentifier id,
      Relationship relationship, SqlNode node) {
    //this could be made cleaner
    String alias = this.analysis.getTableAlias().get(id);
    if (node instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) node;

      //simple, return from condition
      if (select.getFetch() == null && (select.getHints() ==null||select.getHints().getList().isEmpty())) {
        //append condition
        SqlJoin join = (SqlJoin) select.getFrom();
        SqlIdentifier identifier = (SqlIdentifier) join.getRight();

        join.setRight(SqlStdOperatorTable.AS.createCall(
            SqlParserPos.ZERO,
            join.getRight(),
            new SqlIdentifier(alias, SqlParserPos.ZERO)
        ));

        String toReplace = identifier.names.get(0);
        join = (SqlJoin) join.accept(new SqlShuttle() {
          @Override
          public SqlNode visit(SqlIdentifier id) {
            if (id.names.size() == 2) {
              //todo: actually walk tree instead of just guessing
              if (id.names.get(0).equalsIgnoreCase(toReplace)) {
                List<String> names = new ArrayList<>(id.names);
                names.set(0, alias);
                return new SqlIdentifier(names, id.getParserPosition());
              }
            }
            return super.visit(id);
          }
        });


//        SqlCall call = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
//            new SqlIdentifier(List.of(alias,
//                SqlValidatorUtil.getAlias(select.getSelectList().get(i), i)
//            ), SqlParserPos.ZERO),
//            new SqlIdentifier(List.of(resolveRel.getAlias(), pkName), SqlParserPos.ZERO)
//        );
//        pullup.push(call);

        //also add a join

        return join;
      } else {
        return super.visit(id);
      }
//
//      //Is not simple
//      if (select.getFetch() != null) {
//        //todo: get name from first
//        List<SqlNode> conditions = new ArrayList<>();
//
//        for (int i = 0; i < relationship.getFromTable().getVt().getPrimaryKeyNames().size()
//            && i < relationship.getToTable().getVt().getPrimaryKeyNames().size(); i++) {
//
//          String pkName = relationship.getFromTable().getVt().getPrimaryKeyNames().get(i);
//
//          SqlCall call = SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO,
//              new SqlIdentifier(List.of(alias,
//                  SqlValidatorUtil.getAlias(select.getSelectList().get(i), i)
//                  ), SqlParserPos.ZERO),
//              new SqlIdentifier(List.of(resolveRel.getAlias(), pkName), SqlParserPos.ZERO)
//          );
//          conditions.add(call);
//        }
//        SqlNode condition = SqlNodeUtil.and(conditions);
//
//        this.pullup.push(condition);
//        return node;
//      }

    }

    return node;
  }

}
