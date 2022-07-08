package ai.datasqrl.plan.local;

import ai.datasqrl.schema.Relationship;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

public class JoinPathBuilder {
  public Map<Relationship, SqlJoinDeclaration> joinDeclarations = new HashMap<>();

  public boolean terminal = false;
  public String currentAlias;
  public SqlNode sqlNode;
  public SqlNode trailingCondition;

  public void setCurrentAlias(String alias) {
    //Sets the alias as the current context
    currentAlias = alias;
  }

  public void join(Relationship rel) {
    joinInternal(rel, currentAlias);
  }

  public void join(Relationship rel, String alias) {
    //If alias is explicitly defined, it is a terminal state
    terminal = true;
    joinInternal(rel, alias);
  }

  public void joinInternal(Relationship rel, String alias) {
    SqlJoinDeclaration declaration = joinDeclarations.get(rel)
        .rewriteSelfAlias(alias);

    if (sqlNode == null) {
      sqlNode = declaration.getRel();
      trailingCondition = declaration.getCondition();
    } else {
      SqlLiteral conditionType = SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO);
      SqlLiteral joinType = SqlLiteral.createSymbol(org.apache.calcite.sql.JoinType.LEFT, SqlParserPos.ZERO);
      sqlNode = new SqlJoin(SqlParserPos.ZERO, sqlNode, SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
          joinType, declaration.getRel(), conditionType, declaration.getCondition());
    }

    //Last
    setCurrentAlias(declaration.getToTableAlias());
  }

  public SqlNode getSqlNode() {
    return sqlNode;
  }

  public SqlNode getTrailingCondition() {
    return trailingCondition;
  }
}