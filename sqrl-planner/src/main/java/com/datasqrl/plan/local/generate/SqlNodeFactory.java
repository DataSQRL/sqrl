package com.datasqrl.plan.local.generate;

import com.datasqrl.name.ReservedName;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import lombok.Value;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

@Value
public class SqlNodeFactory implements AbstractSqlNodeFactory {

  SqlParserPos baseParserPos;

  @Override
  public SqlNodeList createStarSelectList() {
    return new SqlNodeList(List.of(SqlIdentifier.star(baseParserPos)), baseParserPos);
  }

  @Override
  public SqlNodeList createDistinctOnHintList(List<SqlNode> partitionKeys) {
    return new SqlNodeList(List.of(new SqlHint(baseParserPos,
        new SqlIdentifier("DISTINCT_ON", baseParserPos),
        new SqlNodeList(partitionKeys, baseParserPos),
        HintOptionFormat.ID_LIST
    )), baseParserPos);
  }

  @Override
  public SqlSelect createSqlSelect() {
    return new SqlSelect(baseParserPos,
        SqlNodeList.EMPTY, null, null, null, null, null, SqlNodeList.EMPTY, null,null,null,null);
  }

  @Override
  public SqlNodeList list(SqlNode... nodes) {
    return list(Arrays.asList(nodes));
  }

  @Override
  public SqlNodeList list(List<SqlNode> nodes) {
    return new SqlNodeList(nodes, baseParserPos);
  }

  public SqlCall callAs(SqlNode node, String alias) {
    Objects.requireNonNull(node, "node must not be null");
    Objects.requireNonNull(alias, "alias must not be null");

    return SqlStdOperatorTable.AS.createCall(
        SqlParserPos.ZERO,
        node,
        new SqlIdentifier(List.of(alias), SqlParserPos.ZERO)
    );
  }

  @Override
  public SqlIdentifier toIdentifier(String... names) {
    return toIdentifier(Arrays.asList(names));
  }

  public SqlNodeList prependList(SqlNodeList original, List<SqlNode> toPrepend) {
    List<SqlNode> nodes = new ArrayList<>(toPrepend);
    if (original != null) {
      nodes.addAll(original.getList());
    }
    return new SqlNodeList(nodes, toPrepend.get(0).getParserPosition());
  }

  @Override
  public SqlNode createSelf() {
    return callAs(toIdentifier(ReservedName.SELF_IDENTIFIER.getCanonical()),
        ReservedName.SELF_IDENTIFIER.getCanonical());
  }

  @Override
  public SqlIdentifier toIdentifier(String name) {
    return toIdentifier(List.of(name));
  }

  @Override
  public SqlIdentifier toIdentifier(List<String> names) {
    return new SqlIdentifier(names, baseParserPos);
  }

  @Override
  public SqlJoin createJoin(SqlNode lhs, JoinType joinType, SqlNode rhs) {
    return new SqlJoin(
        baseParserPos,
        lhs,
        SqlLiteral.createBoolean(false, baseParserPos),
        joinType.symbol(baseParserPos),
        rhs,
        JoinConditionType.NONE.symbol(baseParserPos),
        null
    );
  }

  @Override
  public SqlCall callEq(SqlNode lhs, SqlNode rhs) {
    return SqlStdOperatorTable.EQUALS.createCall(baseParserPos, lhs, rhs);
  }
}
