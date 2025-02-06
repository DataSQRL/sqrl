package com.datasqrl.calcite.schema.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.commons.collections.ListUtils;

import com.datasqrl.calcite.SqrlToSql.PullupColumn;
import com.datasqrl.plan.hints.TopNHint;
import com.datasqrl.plan.hints.TopNHint.Type;
import com.google.common.base.Preconditions;

import lombok.experimental.UtilityClass;

@UtilityClass
public class SqlBuilders {
  public static class SqlAliasCallBuilder {

    private final SqlCall node;

    public SqlAliasCallBuilder(SqlCall node) {
      Preconditions.checkState(node.getKind() == SqlKind.AS);
      this.node = node.getOperator().createCall(SqlParserPos.ZERO,
          node.getOperandList().subList(0, node.getOperandList().size()).toArray(SqlNode[]::new));
    }

    public String getAlias() {
      var alias = ((SqlIdentifier)node.getOperandList().get(1)).getSimple();
      return alias;
    }

    public SqlAliasCallBuilder setTable(SqlNode sqlNode) {
      if (sqlNode.getKind() == SqlKind.AS) {
        sqlNode = ((SqlCall) sqlNode).getOperandList().get(0);
      }

      this.node.setOperand(0, sqlNode);
      return this;
    }

    public SqlNode build() {
      return node;
    }
  }

  public static class SqlJoinBuilder {

    private final SqlJoin join;

    public SqlJoinBuilder(SqlJoin call) {
      this.join = new SqlJoin(SqlParserPos.ZERO,
          call.operand(0),
          call.operand(1),
          call.operand(2),
          call.operand(3),
          shouldAddOnCondition(call)
              ? JoinConditionType.ON.symbol(SqlParserPos.ZERO)
              : call.operand(4),
          shouldAddOnCondition(call)
              ? SqlLiteral.createBoolean(true, SqlParserPos.ZERO)
              : call.operand(5),
          call.operand(6)
          );
    }

    public boolean shouldAddOnCondition(SqlJoin call) {
      return call.operand(4).equals(JoinConditionType.NONE.symbol(SqlParserPos.ZERO))
          && !call.isNatural() && call.getJoinType() != JoinType.COMMA
          && call.getJoinType() != JoinType.CROSS;
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
      if (join.getCondition() != null) {
        join.setOperand(5, join.getCondition().accept(shuttle));
      }
      return this;
    }
  }

  public static class SqlSelectBuilder {

    private final SqlSelect select;

    public SqlSelectBuilder() {
      this(SqlParserPos.ZERO);
    }

    public SqlSelectBuilder(SqlParserPos pos) {
      this.select = new SqlSelect(pos,
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
      var hint = TopNHint.createSqlHint(type,
          new SqlNodeList(keyNodes, SqlParserPos.ZERO), SqlParserPos.ZERO);

      List<SqlNode> hints = new ArrayList<>(select.getHints().getList());
      hints.add(hint);
      select.setHints(new SqlNodeList(hints, SqlParserPos.ZERO));
      return this;
    }

    public static List<SqlNode> sqlIntRange(Set<Integer> keySet) {
      return keySet.stream()
          .map(i -> new SqlIdentifier(Integer.toString(i), SqlParserPos.ZERO))
          .collect(Collectors.toList());
    }

    public SqlSelectBuilder clearKeywords() {
      var keywords = SqlNodeList.EMPTY;
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

    public void prependSelect(List<PullupColumn> keysToPullUp) {
      select.setSelectList(prepend(select.getSelectList(), mapToSelectIdentifier(keysToPullUp)));
    }

    private List<SqlNode> mapToIdentifier(List<PullupColumn> keysToPullUp) {
      return keysToPullUp.stream()
          .map(n->new SqlIdentifier(n.getDisplayName(), SqlParserPos.ZERO))
          .collect(Collectors.toList());
    }

    private List<SqlNode> mapToSelectIdentifier(List<PullupColumn> keysToPullUp) {
      return keysToPullUp.stream()
          .map(n->
          SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
                  new SqlIdentifier(n.getDisplayName(), SqlParserPos.ZERO),
                  new SqlIdentifier(n.getFinalName(), SqlParserPos.ZERO)))
          .collect(Collectors.toList());
    }

    private SqlNodeList prepend(SqlNodeList selectList, List<SqlNode> keysToPullUp) {
      return new SqlNodeList(ListUtils.union(keysToPullUp, selectList.getList()), selectList.getParserPosition());
    }

    public void prependGroup(List<PullupColumn> keysToPullUp) {
      select.setGroupBy(prepend(select.getGroup() == null ? SqlNodeList.EMPTY:select.getGroup(),
          mapToIdentifier(keysToPullUp)));
    }
    public void prependOrder(List<PullupColumn> keysToPullUp) {
      select.setOrderBy(prepend(select.getOrderList() == null? SqlNodeList.EMPTY:select.getOrderList(),
          mapToIdentifier(keysToPullUp)));
    }

    public boolean hasGroup() {
      return select.getGroup() != null && !select.getGroup().getList().isEmpty();
    }

    public boolean hasOrder() {
      return select.getOrderList() != null && !select.getOrderList().getList().isEmpty();
    }

    public SqlSelectBuilder rewriteExpressions(SqlShuttle shuttle) {
      select.setSelectList((SqlNodeList) select.getSelectList().accept(shuttle));
      if (select.getWhere() != null) {
		select.setWhere(select.getWhere().accept(shuttle));
	}
      if (select.getGroup() != null) {
		select.setGroupBy((SqlNodeList) select.getGroup().accept(shuttle));
	}
      if (select.getHaving() != null) {
		select.setHaving(select.getHaving().accept(shuttle));
	}
      if (select.getOrderList() != null) {
		select.setOrderBy((SqlNodeList) select.getOrderList().accept(shuttle));
	}

      return this;
    }

    public SqlSelectBuilder setSelectList(List<SqlNode> columns) {
      select.setSelectList(new SqlNodeList(columns, SqlParserPos.ZERO));
      return this;
    }

    public SqlSelectBuilder setLimit(int limit) {
      select.setFetch(SqlLiteral.createExactNumeric(Integer.toString(limit), SqlParserPos.ZERO));
      return this;
    }

    public SqlSelectBuilder setDistinctOnHint(List<Integer> hintOps) {
      List<SqlNode> range = hintOps.stream()
          .map(i -> new SqlIdentifier(Integer.toString(i), SqlParserPos.ZERO))
          .collect(Collectors.toList());
      var hint = new SqlHint(SqlParserPos.ZERO, new SqlIdentifier("DISTINCT_ON", SqlParserPos.ZERO),
          new SqlNodeList(range, SqlParserPos.ZERO), HintOptionFormat.ID_LIST);
      select.setHints(new SqlNodeList(List.of(hint), SqlParserPos.ZERO));

      return this;
    }

    public SqlSelectBuilder setWhere(List<SqlNode> conditions) {
      var call = SqlUtil.createCall(SqlStdOperatorTable.AND, SqlParserPos.ZERO, conditions);
      select.setWhere(call);
      return this;
    }

    public SqlSelectBuilder clearHints() {
      select.setHints(SqlNodeList.EMPTY);
      return this;
    }

    public void appendWhere(SqlNode sqlNode) {
      List<SqlNode> nodes = new ArrayList<>();
      if (select.getWhere() != null) {
		nodes.add(select.getWhere());
	}
      nodes.add(sqlNode);

      this.setWhere(nodes);
    }
  }

  public static class SqlCallBuilder {

    private SqlCall call;

    public SqlCallBuilder(SqlCall call) {
      this.call = call;
    }

    public SqlCall build() {
      return call;
    }

    public SqlCallBuilder setOperands(List<SqlNode> operands) {
      call = call.getOperator().createCall(call.getParserPosition(), operands);
      return this;
    }
  }
}
