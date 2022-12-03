package com.datasqrl.plan.local.transpile;

import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlJoinPath;
import org.apache.calcite.sql.SqrlJoinSetOperation;
import org.apache.calcite.sql.SqrlJoinTerm;
import org.apache.calcite.sql.SqrlJoinTerm.SqrlJoinTermVisitor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Qualifies all identifiers that could be potentially ambiguous during transpilation
 *
 * Orders.entries2 := SELECT *
 *                    FROM _.parent.entries;
 * ->
 * Orders.entries2 := SELECT x1.discount, x1.unit_price, ...
 *                    FROM _.parent.entries AS x1;
 */
public class QualifyIdentifiers extends SqlShuttle
  implements SqrlJoinTermVisitor<SqrlJoinTerm, Object> {


  private final Analysis analysis;

  public QualifyIdentifiers(Analysis analysis) {
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
  public SqlNode visit(SqlCall node) {
    switch (node.getKind()) {
      case SELECT:
        return rewriteSelect((SqlSelect) node);
      case JOIN_DECLARATION:
        return rewriteJoinDeclaration((SqrlJoinDeclarationSpec) node);
    }

    return super.visit(node);
  }

  private SqlNode rewriteJoinDeclaration(SqrlJoinDeclarationSpec node) {
    SqrlJoinTerm relation = node.getRelation().accept(this, null);
    Optional<SqlNodeList> orders = node.getOrderList().map(o->(SqlNodeList)expr(o));

    return new SqrlJoinDeclarationSpec(
        node.getParserPosition(),
        relation,
        orders,
        node.fetch,
        node.inverse,
        node.leftJoins
    );
  }

  @Override
  public SqrlJoinTerm visitJoinPath(SqrlJoinPath sqrlJoinPath, Object context) {
    List<SqlNode> relations = new ArrayList<>();
    List<SqlNode> conditions = new ArrayList<>();
    for (int i = 0; i < sqrlJoinPath.relations.size(); i++) {
      relations.add(sqrlJoinPath.relations.get(i).accept(this));
      if (sqrlJoinPath.getConditions().get(i) != null) {
        conditions.add(expr(sqrlJoinPath.getConditions().get(i)));
      } else {
        conditions.add(null);
      }
    }
    return new SqrlJoinPath(sqrlJoinPath.getParserPosition(),
        relations,
        conditions);
  }

  @Override
  public SqrlJoinTerm visitJoinSetOperation(SqrlJoinSetOperation sqrlJoinSetOperation,
      Object context) {
    return null;
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
    List<SqlNode> full = analysis.groupByExpressions.get(group)
        .stream()
        .map(this::expr)
        .collect(Collectors.toList());

    return new SqlNodeList(full, group.getParserPosition());
  }

  public SqlNode expr(SqlNode node) {
    return node.accept(new QualifyExpression());
  }

  public class QualifyExpression extends SqlShuttle {

    @Override
    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {

        case SELECT:
        case UNION:
        case INTERSECT:
        case EXCEPT:
          QualifyIdentifiers qualifyIdentifiers = new QualifyIdentifiers(analysis);

          return call.accept(qualifyIdentifiers);

      }
      return super.visit(call);
    }

    @Override
    public SqlNode visit(SqlIdentifier id) {
      if (analysis.getExpressions().containsKey(id)) {
        return analysis.getExpressions().get(id).getAliasedIdentifier(id);
      }
      
      return super.visit(id);
    }
  }
}
