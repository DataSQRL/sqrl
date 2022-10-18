package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.hints.TopNHint;
import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.local.HasToTable;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.schema.Relationship;
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
public class JoinDeclarationFactory {

  private final Env env;
  RexBuilder rexBuilder;

  public JoinDeclarationFactory(Env env) {
    this.env = env;
    this.rexBuilder = env.getSession().getPlanner().getRelBuilder().getRexBuilder();
  }

  public SqlJoinDeclaration create(TableWithPK pkTable, RelNode relNode, SqlNode sqlNode) {
    Optional<SqlHint> hint = Optional.empty();
    if (relNode instanceof LogicalSort &&
        ((LogicalSort) relNode).fetch != null) {
      List<SqlNode> pksIndices = IntStream.range(0, pkTable.getPrimaryKeyNames().size())
          .mapToObj(i -> new SqlIdentifier(
              Long.toString(i),
              SqlParserPos.ZERO))
          .collect(Collectors.toList());
      hint = Optional.of(new SqlHint(SqlParserPos.ZERO,
          new SqlIdentifier(TopNHint.Type.TOP_N.name(), SqlParserPos.ZERO),
          new SqlNodeList(pksIndices, SqlParserPos.ZERO),
          HintOptionFormat.ID_LIST));
    }

    SqlBasicCall tRight = (SqlBasicCall) getRightDeepTable(sqlNode);

    SqlNode join = unwrapSelect(sqlNode).getFrom();

    String lastAlias = Util.last(((SqlIdentifier) tRight.getOperandList().get(1)).names);
    if (relNode instanceof LogicalSort &&
        ((LogicalSort) relNode).fetch != null) {
      SqlSelect select = unwrapSelect(sqlNode);
      hint.ifPresent(h -> select.setHints(new SqlNodeList(List.of(h), SqlParserPos.ZERO)));
      select.setSelectList(new SqlNodeList(List.of(
          //todo: more than 1 pk
          select.getSelectList().get(0),
          new SqlIdentifier(List.of(lastAlias, ""), SqlParserPos.ZERO)),
          SqlParserPos.ZERO));
      String a = env.getAliasGenerator().generateFieldName();
      SqlBasicCall alias = new SqlBasicCall(SqlStdOperatorTable.AS,
          new SqlNode[]{
              select,
              new SqlIdentifier(a, SqlParserPos.ZERO)
          }, SqlParserPos.ZERO);
      //change condition to be on pk
      List<SqlNode> pks = new ArrayList<>();
      for (String pk : pkTable.getPrimaryKeyNames()) {
        pks.add(new SqlBasicCall(SqlStdOperatorTable.EQUALS,
            new SqlNode[]{
                new SqlIdentifier(List.of("_", pk), SqlParserPos.ZERO),
                new SqlIdentifier(List.of(a,
                    Util.last(((SqlIdentifier) ((SqlBasicCall) select.getSelectList()
                        .get(0)).getOperandList().get(1))
                        .names)
                ), SqlParserPos.ZERO)
            }, SqlParserPos.ZERO
        ));
      }
      SqlJoinDeclaration dec = new SqlJoinDeclarationImpl(
          Optional.ofNullable(and(pks)),
          alias,
          "_",
          a);
      return dec;
    } else {
      SqlJoin join1 = (SqlJoin) join;
      SqlJoinDeclaration dec = new SqlJoinDeclarationImpl(
          Optional.of(join1.getCondition()),
          join1.getRight(),
          "_",
          ((SqlIdentifier) tRight.getOperandList().get(1)).names.get(0));
      return dec;
    }

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

  public SQRLTable getToTable(SqlValidator validator, SqlNode sqlNode) {

    SqlNode tRight = getRightDeepTable(sqlNode);
    HasToTable table = validator.getNamespace(tRight).getTable()
        .unwrap(HasToTable.class);
    return table.getToTable();
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

  protected SqlNode createParentChildCondition(Relationship rel, String alias,
      Env env) {
    TableWithPK child = rel.getJoinType().equals(Relationship.JoinType.PARENT) ? env.getTableMap().get(
        rel.getFromTable()) : env.getTableMap().get(rel.getToTable());
    TableWithPK parent = rel.getJoinType().equals(Relationship.JoinType.PARENT) ? env.getTableMap().get(
        rel.getToTable()) : env.getTableMap().get(rel.getFromTable());

    List<SqlNode> conditions = new ArrayList<>();
    for (int i = 0; i < parent.getPrimaryKeyNames().size(); i++) {
      String childPk = child.getPrimaryKeyNames().get(i);
      String parentPk = parent.getPrimaryKeyNames().get(i);
      conditions.add(new SqlBasicCall(SqlStdOperatorTable.EQUALS,
          new SqlNode[]{new SqlIdentifier(List.of("_", childPk), SqlParserPos.ZERO),
              new SqlIdentifier(List.of(alias, parentPk), SqlParserPos.ZERO)}, SqlParserPos.ZERO));
    }

    return and(conditions);
  }

  public SqlJoinDeclaration createParentChildJoinDeclaration(Relationship rel) {
    TableWithPK pk = env.getTableMap().get(rel.getToTable());
    String alias = env.getAliasGenerator().generate(pk);
    return new SqlJoinDeclarationImpl(
        Optional.of(createParentChildCondition(rel, alias, env)),
        createTableRef(rel.getToTable(), alias, env), "_", alias);
  }


  private SqlNode createTableRef(SQRLTable table, String alias, Env tableMapper) {
    return new SqlBasicCall(SqlStdOperatorTable.AS, new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
        new SqlIdentifier(env.getTableMap().get(table).getNameId(), SqlParserPos.ZERO),
        SqlNodeList.EMPTY), new SqlIdentifier(alias, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
  }
}
