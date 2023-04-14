/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.transpile;

import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.SQRLTable;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlJoinPath;
import org.apache.calcite.sql.SqrlJoinSetOperation;
import org.apache.calcite.sql.validate.SqlValidator;

@AllArgsConstructor
public class JoinDeclarationUtil {

  RexBuilder rexBuilder;

  public SqlNode getRightDeepTable(SqlNode node) {
    if (node instanceof SqrlJoinDeclarationSpec) {
      return getRightDeepTable(((SqrlJoinDeclarationSpec) node).getRelation());
    } else if (node instanceof SqlBasicCall && ((SqlBasicCall)node).getKind() == SqlKind.AS) {
      return ((SqlBasicCall) node).getOperandList().get(0);
    } else if (node instanceof SqrlJoinPath) {
      return getRightDeepTable(((SqrlJoinPath) node).relations.get(((SqrlJoinPath) node).getRelations().size()-1));
    } else if (node instanceof SqrlJoinSetOperation) {
      throw new RuntimeException("not yet implemented");
    } else if (node instanceof SqlSelect) {
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
    SqlIdentifier identifier = (SqlIdentifier) tRight;
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
