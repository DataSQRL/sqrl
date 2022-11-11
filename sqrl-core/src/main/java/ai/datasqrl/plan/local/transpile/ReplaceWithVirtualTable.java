package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.util.SqlNodeUtil;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.AbsoluteResolvedTable;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.RelativeResolvedTable;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.ResolvedTable;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.ResolvedTableField;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.SingleTable;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.SQRLTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlJoinPath;
import org.apache.calcite.sql.SqrlJoinSetOperation;
import org.apache.calcite.sql.SqrlJoinTerm;
import org.apache.calcite.sql.SqrlJoinTerm.SqrlJoinTermVisitor;
import org.apache.calcite.sql.UnboundJoin;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.lang3.tuple.Pair;

public class ReplaceWithVirtualTable extends SqlShuttle
    implements SqrlJoinTermVisitor<SqrlJoinTerm, Object> {

  private final Analysis analysis;

  Stack<SqlNode> pullup = new Stack<>();

  AtomicInteger aliasCnt = new AtomicInteger(0);

  public ReplaceWithVirtualTable(Analysis analysis) {

    this.analysis = analysis;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case UNBOUND_JOIN:
        UnboundJoin u = (UnboundJoin)call;
        return new UnboundJoin(u.getParserPosition(),
            u.getRelation().accept(this),
            u.getCondition()
        );
      case JOIN_DECLARATION:
        SqrlJoinDeclarationSpec spec = (SqrlJoinDeclarationSpec) call;
        SqrlJoinTerm relation = spec.getRelation().accept(this, null);
        Optional<SqlNodeList> orderList = spec.getOrderList().map(o->(SqlNodeList)o.accept(this));
        Optional<SqlNodeList> leftJoin = spec.getLeftJoins().map(l->(SqlNodeList) l.accept(this));

        return new SqrlJoinDeclarationSpec(spec.getParserPosition(),
            relation,
            orderList,
            spec.getFetch(),
            spec.getInverse(),
            leftJoin);
      case AS:
        SqlNode tbl = call.getOperandList().get(0);
        //aliased in inlined
        if (tbl instanceof SqlIdentifier) {
          ResolvedTable resolved = analysis.getTableIdentifiers().get(tbl);
          if (resolved instanceof RelativeResolvedTable &&
              ((RelativeResolvedTable)resolved).getFields().get(0).getJoin().isPresent()){
            return tbl.accept(this);
          }
        }

        return super.visit(call);

      case SELECT:
        SqlSelect select = (SqlSelect) super.visit(call);
        while (!pullup.isEmpty()) {
          SqlNode condition = pullup.pop();

          appendToSelect(select, condition);
        }
        return select;
      case JOIN:
        SqlJoin join = (SqlJoin) super.visit(call);
        if (!pullup.isEmpty()) {
          SqlNode condition = pullup.pop();
          addJoinCondition(join, condition);
        }

        return join;
    }

    return super.visit(call);
  }

  public static void addJoinCondition(SqlJoin join, SqlNode condition) {
    if (join.getCondition() == null) {
      join.setOperand(5, condition);
    } else {
      join.setOperand(5, SqlNodeUtil.and(join.getCondition(), condition));
    }
    join.setOperand(4, JoinConditionType.ON.symbol(SqlParserPos.ZERO));
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
    //identifier
    if (analysis.getExpressions().get(id) != null) {
      return id.accept(new ShadowColumns());
    } else if (analysis.getTableIdentifiers().get(id) == null) {
      return super.visit(id);
    }

    ResolvedTable resolved = analysis.getTableIdentifiers().get(id);
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
      //Is a join declaration
      if (relationship.getJoin().isPresent()) {
        String alias = analysis.tableAlias.get(id);

        ExpandJoinDeclaration expander = new ExpandJoinDeclaration(resolveRel.getAlias(), alias, aliasCnt);
        UnboundJoin pair = expander.expand(relationship.getJoin().get());
        pair.getCondition().map(c->pullup.push(c));
        return pair.getRelation();
      }

      String alias = analysis.tableAlias.get(id);
      SqlNode condition = createCondition(alias, resolveRel.getAlias(),
          relationship.getFromTable(),
          relationship.getToTable()
      );
      pullup.push(condition);

      return new SqlIdentifier(relationship.getToTable().getVt().getNameId(),
          id.getParserPosition());
    }

    return super.visit(id);
  }

  private SqlNode pullWhereIntoJoin(SqlNode query) {
    //
    if (query instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) query;
      if (select.getWhere() != null) {
        SqlJoin join = (SqlJoin) select.getFrom();
        FlattenTablePaths.addJoinCondition(join, select.getWhere());
      }
      return select.getFrom();
    }
    return query;
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

  @Override
  public SqrlJoinTerm visitJoinPath(SqrlJoinPath node, Object context) {
    List<SqlNode> relations = new ArrayList<>();
    List<SqlNode> conditions = new ArrayList<>();
    for (int i = 0; i < node.getRelations().size(); i++) {
      relations.add(node.getRelations().get(i).accept(this));
      if (node.getConditions().get(i) != null) {
        conditions.add(node.getConditions().get(i).accept(this));
      } else {
        conditions.add(null);
      }
      if (!pullup.isEmpty()) {
        SqlNode condition = pullup.pop();
        conditions.set(i, SqlNodeUtil.and(conditions.get(i), condition));
      }
    }

    return new SqrlJoinPath(node.getParserPosition(),
        relations,
        conditions);
  }

  @Override
  public SqrlJoinTerm visitJoinSetOperation(SqrlJoinSetOperation sqrlJoinSetOperation,
      Object context) {
    return null;
  }

  private class ShadowColumns extends SqlShuttle {

    @Override
    public SqlNode visit(SqlIdentifier id) {
      ResolvedTableField field = analysis.getExpressions().get(id);
      if (field != null) {
        return field.getShadowedIdentifier(id);
      }
      return super.visit(id);
    }
  }
}
