package com.datasqrl.plan.local.generate;


import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.calcite.sql.ImportDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

public class ConvertTimestampToQuery {

  public SqlNode convert(ImportDefinition statement) {
    // Preconditions check to make sure statement is not null
    Preconditions.checkNotNull(statement, "Statement must not be null");

    // Create an SqlNode either with alias or without
    SqlNode sqlNode = createSqlNodeWithAlias(statement);

    // Transform expression to query
    return transformExpressionToQuery(sqlNode);
  }

  private SqlNode createSqlNodeWithAlias(ImportDefinition statement) {
    return statement.getTimestampAlias()
        .map(alias -> (SqlNode)SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, statement.getTimestamp().get(), alias))
        .orElseGet(statement.getTimestamp()::get);
  }

  private SqlNode transformExpressionToQuery(SqlNode sqlNode) {
//    checkState(getContext(env, statement).isPresent(), ErrorCode.MISSING_DEST_TABLE, statement,
//        String.format("Could not find table: %s", statement.getNamePath()));
    return new SqlSelect(SqlParserPos.ZERO,
        SqlNodeList.EMPTY,
        new SqlNodeList(List.of(sqlNode), SqlParserPos.ZERO),
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
}
