package ai.datasqrl.plan.local;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.local.analyzer.Analysis;
import ai.datasqrl.plan.local.analyzer.Analysis.ResolvedNamePath;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.parser.SqlParserPos;

public class JoinPathBuilder {
  public Map<Relationship, SqlJoinDeclaration> joinDeclarations;

  public JoinPathBuilder(Map<Relationship, SqlJoinDeclaration> joinDeclarations) {
    this.joinDeclarations = joinDeclarations;
  }

  public boolean terminal = false;
  public String currentAlias;
  public SqlNode sqlNode;
  public Optional<SqlNode> trailingCondition = Optional.empty();

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
    Preconditions.checkNotNull(joinDeclarations.get(rel), "Could not find declaration", rel);
    SqlJoinDeclaration declaration = joinDeclarations.get(rel)
        .rewriteSelfAlias(alias);

    if (sqlNode == null) {
      sqlNode = declaration.getRel();
      trailingCondition = Optional.of(declaration.getCondition());
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

  public Optional<SqlNode> getTrailingCondition() {
    return trailingCondition;
  }

  public SqlJoin resolveFull(ResolvedNamePath namePath) {
    String tableAlias = "_"; //todo: fix
    this.setCurrentAlias(tableAlias);
    for (Field field : namePath.getPath()) {
      if (field instanceof Relationship) {
        this.join((Relationship) field);
      }
    }

//    SqlJoinDeclaration declaration = new SqlJoinDeclaration(this.getSqlNode(), this.getTrailingCondition().get());
//    declaration.rewriteSelfAlias("_");

    SqlLiteral conditionType = SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO);
    SqlLiteral joinType = SqlLiteral.createSymbol(org.apache.calcite.sql.JoinType.LEFT, SqlParserPos.ZERO);

    return new SqlJoin(SqlParserPos.ZERO,
        new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{
            new SqlTableRef(SqlParserPos.ZERO,
                new SqlIdentifier(namePath.getBase().get().getToTable().getId().getCanonical(),
                    SqlParserPos.ZERO), SqlNodeList.EMPTY),
            new SqlIdentifier(tableAlias, SqlParserPos.ZERO)
          }, SqlParserPos.ZERO),
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        joinType,
        getSqlNode(),
        conditionType,
        getTrailingCondition().get()
    );
  }

  //return result
}