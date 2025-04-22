package com.datasqrl.plan.local.generate;

import java.util.List;

import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

public interface AbstractSqlNodeFactory {

  SqlNodeList createStarSelectList();

  SqlNodeList createDistinctOnHintList(List<SqlNode> partitionKeys);

  SqlSelect createSqlSelect();

  SqlNodeList list(SqlNode... nodes);

  SqlNodeList list(List<SqlNode> nodes);

  org.apache.calcite.sql.parser.SqlParserPos getBaseParserPos();

  SqlCall callAs(SqlNode node, String alias);

  SqlIdentifier toIdentifier(String... names);

  SqlNodeList prependList(SqlNodeList original, List<SqlNode> toPrepend);

  SqlNode createSelf();

  SqlIdentifier toIdentifier(String name);

  SqlIdentifier toIdentifier(List<String> names);

  SqlJoin createJoin(SqlNode lhs, JoinType joinType, SqlNode rhs);

  SqlCall callEq(SqlNode lhs, SqlNode rhs);
}
