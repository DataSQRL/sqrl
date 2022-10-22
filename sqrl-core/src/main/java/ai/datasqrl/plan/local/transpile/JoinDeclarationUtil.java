package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.hints.TopNHint;
import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.SQRLTable;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ai.datasqrl.plan.calcite.util.SqlNodeUtil.and;

@AllArgsConstructor
public class JoinDeclarationUtil {

  RexBuilder rexBuilder;

  public JoinDeclarationUtil(Env env) {
    this.rexBuilder = env.getSession().getPlanner().getRelBuilder().getRexBuilder();
  }

  public SqlNode getRightDeepTable(SqlNode node) {
    if (node instanceof SqlSelect) {
      return getRightDeepTable(((SqlSelect) node).getFrom());
    } else if (node instanceof SqlOrderBy) {
      return getRightDeepTable(((SqlOrderBy) node).query);
    } else if (node instanceof SqlJoin) {
      return getRightDeepTable(((SqlJoin) node).getRight());
    } else {
      return node;
    }
  }

  //todo: fix for union etc
  private SqlSelect unwrapSelect(SqlNode sqlNode) {
    if (sqlNode instanceof SqlOrderBy) {
      return (SqlSelect) ((SqlOrderBy) sqlNode).query;
    }
    return (SqlSelect) sqlNode;
  }

  //TOdo remove baked in assumptions
  public SQRLTable getToTable(SqlValidator validator, SqlNode sqlNode) {

    SqlNode tRight = getRightDeepTable(sqlNode);
    if (tRight.getKind() == SqlKind.AS) {
      tRight = ((SqlCall)tRight).getOperandList().get(0);
    }
    SqlIdentifier identifier = (SqlIdentifier)tRight;
    return validator.getCatalogReader().getTable(identifier.names)
        .unwrap(VirtualRelationalTable.class)
        .getSqrlTable();
  }

  public Multiplicity deriveMultiplicity(RelNode relNode) {
    Multiplicity multiplicity = relNode instanceof LogicalSort &&
        ((LogicalSort) relNode).fetch != null &&
        ((LogicalSort) relNode).fetch.equals(
            rexBuilder.makeExactLiteral(
                BigDecimal.ONE)) ?
        Multiplicity.ONE
        : Multiplicity.MANY;
    return multiplicity;
  }

}
