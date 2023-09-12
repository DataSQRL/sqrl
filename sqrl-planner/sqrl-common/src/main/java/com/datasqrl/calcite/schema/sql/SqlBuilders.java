package com.datasqrl.calcite.schema.sql;

import com.datasqrl.plan.hints.TopNHint;
import com.datasqrl.plan.hints.TopNHint.Type;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.collections.ListUtils;

public class SqlBuilders {
  public static class SqlAliasCallBuilder {

    private final SqlCall node;

    public SqlAliasCallBuilder(SqlCall node) {
      Preconditions.checkState(node.getKind() == SqlKind.AS);
      this.node = node.getOperator().createCall(SqlParserPos.ZERO,
          node.getOperandList().get(0),
          node.getOperandList().get(1));
    }

    public String getAlias() {
      String alias = ((SqlIdentifier)node.getOperandList().get(1)).getSimple();
      return alias;
    }

    public SqlAliasCallBuilder setTable(SqlNode sqlNode) {
      this.node.setOperand(0, sqlNode);
      return this;
    }

    public SqlNode build() {
      return node;
    }
  }

  public static class SqlJoinBuilder {

    private final SqlJoin join;

    public SqlJoinBuilder() {
      this(new SqlJoin(SqlParserPos.ZERO, null, SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
          JoinType.DEFAULT.symbol(SqlParserPos.ZERO),null,
          JoinConditionType.NONE.symbol(SqlParserPos.ZERO),null));
    }
    public SqlJoinBuilder(SqlJoin call) {
      this.join = new SqlJoin(SqlParserPos.ZERO,
          call.operand(0),
          call.operand(1),
          call.operand(2),
          call.operand(3),
          call.operand(4).equals(JoinConditionType.NONE.symbol(SqlParserPos.ZERO))
              ? JoinConditionType.ON.symbol(SqlParserPos.ZERO)
              : call.operand(4),
          call.operand(4).equals(JoinConditionType.NONE.symbol(SqlParserPos.ZERO))
              ? SqlLiteral.createBoolean(true, SqlParserPos.ZERO)
              : call.operand(5));
    }

    public SqlJoinBuilder setLeft(SqlNode sqlNode) {
      join.setLeft(sqlNode);
      return this;
    }

    public SqlJoinBuilder setRight(SqlNode sqlNode) {
      join.setRight(sqlNode);
      return this;
    }

    public SqlJoinBuilder lateral() {
      this.setRight(SqlStdOperatorTable.LATERAL.createCall(SqlParserPos.ZERO, join.getRight()));
      return this;
    }

    public SqlNode build() {
      return join;
    }

    public SqlJoinBuilder rewriteExpressions(SqlShuttle shuttle) {
      join.setOperand(5, join.getCondition().accept(shuttle));
      return this;
    }
  }

  public static class SqlSelectBuilder {

    private final SqlSelect select;

    public SqlSelectBuilder() {
      this.select = new SqlSelect(SqlParserPos.ZERO,
          SqlNodeList.EMPTY,
          new SqlNodeList(List.of(SqlIdentifier.star(SqlParserPos.ZERO)), SqlParserPos.ZERO),
          null,
          null,
          null,
          null,
          SqlNodeList.EMPTY,
          null,
          null,
          null,
          SqlNodeList.EMPTY
      );
    }
    public SqlSelectBuilder(SqlSelect select) {
      this.select = new SqlSelect(select.getParserPosition(),
          select.operand(0),
          select.operand(1),
          select.operand(2),
          select.operand(3),
          select.operand(4),
          select.operand(5),
          select.operand(6),
          select.operand(7),
          select.operand(8),
          select.operand(9),
          select.operand(10));
    }

    public SqlSelectBuilder setTopNHint(Type type, List<SqlNode> keyNodes) {
      SqlHint hint = TopNHint.createSqlHint(type,
          new SqlNodeList(keyNodes, SqlParserPos.ZERO), SqlParserPos.ZERO);

      List<SqlNode> hints = new ArrayList<>(select.getHints().getList());
      hints.add(hint);
      select.setHints(new SqlNodeList(hints, SqlParserPos.ZERO));
      return this;
    }

    public static List<SqlNode> sqlIntRange(int size) {
      return IntStream.range(0, size)
          .mapToObj(i -> new SqlIdentifier(Integer.toString(i), SqlParserPos.ZERO))
          .collect(Collectors.toList());
    }

    public SqlSelectBuilder clearKeywords() {
      SqlNodeList keywords = SqlNodeList.EMPTY;
      select.setOperand(0, keywords);
      return this;
    }

    public SqlSelectBuilder setFrom(SqlNode sqlNode) {
      select.setFrom(sqlNode);
      return this;
    }

    public SqlSelect build() {
      return select;
    }

    public void prependSelect(List<String> keysToPullUp) {
      select.setSelectList(prepend(select.getSelectList(), mapToIdentifier(keysToPullUp)));
    }

    private List<SqlNode> mapToIdentifier(List<String> keysToPullUp) {
      return keysToPullUp.stream()
          .map(n->new SqlIdentifier(n, SqlParserPos.ZERO))
          .collect(Collectors.toList());
    }

    private SqlNodeList prepend(SqlNodeList selectList, List<SqlNode> keysToPullUp) {
      return new SqlNodeList(ListUtils.union(keysToPullUp, selectList.getList()), selectList.getParserPosition());
    }

    public void prependGroup(List<String> keysToPullUp) {
      select.setGroupBy(prepend(select.getGroup() == null ? SqlNodeList.EMPTY:select.getGroup(),mapToIdentifier(keysToPullUp)));
    }
    public void prependOrder(List<String> keysToPullUp) {
      select.setOrderBy(prepend(select.getOrderList() == null? SqlNodeList.EMPTY:select.getOrderList(),mapToIdentifier(keysToPullUp)));
    }

    public boolean hasGroup() {
      return select.getGroup() != null && !select.getGroup().getList().isEmpty();
    }
    public boolean hasOrder() {
      return select.getOrderList() != null && !select.getOrderList().getList().isEmpty();
    }

    public SqlSelectBuilder rewriteExpressions(SqlShuttle shuttle) {
      select.setSelectList((SqlNodeList) select.getSelectList().accept(shuttle));
      if (select.getWhere() != null) select.setWhere(select.getWhere().accept(shuttle));
      if (select.getGroup() != null) select.setGroupBy((SqlNodeList) select.getGroup().accept(shuttle));
      if (select.getHaving() != null) select.setHaving(select.getHaving().accept(shuttle));
      if (select.getOrderList() != null) select.setOrderBy((SqlNodeList) select.getOrderList().accept(shuttle));

      return this;
    }

    public SqlSelectBuilder setSelectList(List<SqlNode> columns) {
      select.setSelectList(new SqlNodeList(columns, SqlParserPos.ZERO));
      return this;
    }
  }
}
