package com.datasqrl.plan.local.transpile;

import com.datasqrl.plan.local.transpile.AnalyzeStatement.AbsoluteResolvedTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.RelativeResolvedTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.ResolvedTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.SingleTable;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.VirtualResolvedTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlJoinPath;
import org.apache.calcite.sql.SqrlJoinSetOperation;
import org.apache.calcite.sql.SqrlJoinTerm;
import org.apache.calcite.sql.SqrlJoinTerm.SqrlJoinTermVisitor;
import org.apache.calcite.sql.UnboundJoin;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Orders.entries.customer = SELECT __a1.customerid FROM @.entries.parent p LEFT JOIN e.parent AS
 * __a1; -> Orders.entries.customer = SELECT __a1.customerid FROM (@.entries AS g1 JOIN g1.parent AS
 * p) LEFT JOIN e.parent AS __a1;
 */
public class FlattenTablePaths extends SqlShuttle
    implements SqrlJoinTermVisitor<SqrlJoinTerm, Object> {

  private final Analysis analysis;

  public FlattenTablePaths(Analysis analysis) {
    this.analysis = analysis;
  }

  public SqlNode accept(SqlNode node) {
    SqlNode result = node.accept(this);
    return result;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case JOIN_DECLARATION:
        SqrlJoinDeclarationSpec spec = (SqrlJoinDeclarationSpec) call;
        SqrlJoinTerm relation = spec.getRelation().accept(this, null);
        Optional<SqlNodeList> leftJoins = spec.getLeftJoins()
            .map(this::convertUnboundJoins);

        return new SqrlJoinDeclarationSpec(spec.getParserPosition(),
            relation,
            spec.getOrderList(),
            spec.getFetch(),
            spec.getInverse(),
            leftJoins);
      case JOIN:
        return super.visit(call);
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

  private SqlNodeList convertUnboundJoins(SqlNodeList l) {
    return new SqlNodeList(l.getList().stream()
        .map(i -> {
          UnboundJoin j = (UnboundJoin) i;
          return new UnboundJoin(i.getParserPosition(), j.getRelation().accept(this),
              j.getCondition());
        })
        .collect(Collectors.toList()), SqlParserPos.ZERO);
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if (analysis.getTableIdentifiers().get(id) != null) {
      return expandTable(id);
    }
    return super.visit(id);
  }

  @Override
  public SqrlJoinTerm visitJoinPath(SqrlJoinPath sqrlJoinPath, Object context) {
    List<SqlNode> relation = new ArrayList<>();
    for (SqlNode rel : sqrlJoinPath.getRelations()) {
      relation.add(rel.accept(this));
    }

    return new SqrlJoinPath(sqrlJoinPath.getParserPosition(),
        relation,
        sqrlJoinPath.getConditions());
  }

  @Override
  public SqrlJoinTerm visitJoinSetOperation(SqrlJoinSetOperation sqrlJoinSetOperation,
      Object context) {
    throw new RuntimeException("SET operations TBD");
  }

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
    ResolvedTable resolve = analysis.tableIdentifiers.get(id);
    if (resolve instanceof SingleTable || resolve instanceof VirtualResolvedTable) {
      suffix = List.of();
      first = new SqlIdentifier(List.of(
          id.names.get(0)
      ), SqlParserPos.ZERO);
    } else if (resolve instanceof AbsoluteResolvedTable) {
      suffix = id.names.subList(1, id.names.size());
      first = new SqlIdentifier(List.of(
          id.names.get(0)
      ), SqlParserPos.ZERO);
    } else if (resolve instanceof RelativeResolvedTable) {
      first = new SqlIdentifier(List.of(id.names.get(0),
          id.names.get(1)
      ), SqlParserPos.ZERO);
      if (id.names.size() > 2) {
        suffix = id.names.subList(2, id.names.size());
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
            new SqlIdentifier(suffix.size() > 0 ? firstAlias : finalAlias,
                SqlParserPos.ZERO));
    if (suffix.size() >= 1) {
      String currentAlias = firstAlias;

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
    }

    return n;
  }
}
