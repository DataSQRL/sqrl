package ai.datasqrl.plan.local.generate.node.builder;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
import ai.datasqrl.plan.local.RootTableField;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.generate.node.SqlJoinDeclaration;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.ScriptTable;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * SQRL allows constructing bushy join trees using a simpler syntax than conventional
 * sql. This is desirable since certain queries can only be accomplished
 * using join trees. For example:
 *
 * -- Customers orders with entries that have a product
 * (assuming product is optional)
 * FROM Customer.orders o LEFT JOIN o.entries.product p;
 *
 * We want to do our best to construct a good join tree:
 *
 *          /       \ (left + order->entries join condition or declaration expansion + user defined)
 *         /     /     \
 *        /\  entries  product
 * Customer orders
 *
 */
public class JoinPathBuilder {
  public Map<Relationship, SqlJoinDeclaration> joinDeclarations;

  private String currentAlias;
  public SqlNode sqlNode;
  public Optional<SqlNode> trailingCondition = Optional.empty();
  private Map<ScriptTable, AbstractSqrlTable> tableMap;

  public JoinPathBuilder(Map<Relationship, SqlJoinDeclaration> joinDeclarations, Map<ScriptTable, AbstractSqrlTable> tableMap) {
    this.joinDeclarations = joinDeclarations;
    this.tableMap = tableMap;
  }

  private void join(Relationship rel) {
    joinInternal(rel, currentAlias, Optional.empty());
  }

  private void joinInternal(Relationship rel, String alias, Optional<String> targetAlias) {
    Preconditions.checkNotNull(joinDeclarations.get(rel), "Could not find declaration", rel);
    SqlJoinDeclaration declaration = joinDeclarations.get(rel)
        .rewriteSelfAlias(alias, targetAlias);

    if (sqlNode == null) {
      sqlNode = declaration.getRel();
      trailingCondition = declaration.getTrailingCondition();
    } else {
      SqlLiteral conditionType;
      if (declaration.getTrailingCondition().isPresent()) {
        conditionType = SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO);
      } else {
        conditionType = SqlLiteral.createSymbol(JoinConditionType.NONE, SqlParserPos.ZERO);
      }

      SqlLiteral joinType = SqlLiteral.createSymbol(JoinType.INNER, SqlParserPos.ZERO);
      sqlNode = new SqlJoin(SqlParserPos.ZERO, sqlNode, SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
          joinType, declaration.getRel(), conditionType, declaration.getTrailingCondition().orElse(null));
    }

    //Last
    currentAlias = declaration.getToTableAlias();
  }

  public SqlNode getSqlNode() {
    return sqlNode;
  }

  public Optional<SqlNode> getTrailingCondition() {
    return trailingCondition;
  }

  /**
   * Subqueries also need to resolve the alias as a table
   */
  public SqlJoinDeclaration expandSubquery(ResolvedNamePath namePath) {
    String tableAlias = "_"; //todo: fix
    currentAlias = tableAlias;
    for (Field field : namePath.getPath()) {
      if (field instanceof RootTableField) {
        RootTableField root = (RootTableField) field;
        String t2 = namePath.getAlias();
        this.sqlNode =  new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{
            new SqlTableRef(SqlParserPos.ZERO,
                new SqlIdentifier(tableMap.get(root.getTable()).getNameId(),
                    SqlParserPos.ZERO), SqlNodeList.EMPTY),
            new SqlIdentifier(t2, SqlParserPos.ZERO)
        }, SqlParserPos.ZERO);
      }
      if (field instanceof Relationship) {
        this.join((Relationship) field);
      }
    }

//    SqlJoinDeclaration declaration = new SqlJoinDeclaration(this.getSqlNode(), this.getTrailingCondition().get());
//    declaration.rewriteSelfAlias("_");

    SqlLiteral conditionType = SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO);
    SqlLiteral joinType = SqlLiteral.createSymbol(org.apache.calcite.sql.JoinType.LEFT, SqlParserPos.ZERO);

    AbstractSqrlTable toTable = tableMap.get(namePath.getBase().get().getToTable());
    return new SqlJoinDeclaration(new SqlJoin(SqlParserPos.ZERO,
        new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{
            new SqlTableRef(SqlParserPos.ZERO,
                new SqlIdentifier(toTable.getNameId(),
                    SqlParserPos.ZERO), SqlNodeList.EMPTY),
            new SqlIdentifier(tableAlias, SqlParserPos.ZERO)
          }, SqlParserPos.ZERO),
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        joinType,
        getSqlNode(),
        conditionType,
        getTrailingCondition().get()
    ), Optional.empty());
  }

  public SqlJoinDeclaration expand(ResolvedNamePath namePath,
      Optional<Name> lastAlias) {
    //_.orders.entries
    if (namePath.getBase().isPresent()) {
      currentAlias = namePath.getAlias();
    } else {
      currentAlias = namePath.getAlias();
    }

    List<Field> path = namePath.getPath();
    for (int i = 0; i < path.size(); i++) {
      Field field = path.get(i);
      if (field instanceof RootTableField) {
        RootTableField root = (RootTableField) field;
        String tableAlias = namePath.getAlias();
        this.sqlNode = new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{
            new SqlTableRef(SqlParserPos.ZERO,
                new SqlIdentifier(tableMap.get(root.getTable()).getNameId(),
                    SqlParserPos.ZERO), SqlNodeList.EMPTY),
            new SqlIdentifier(tableAlias, SqlParserPos.ZERO)
        }, SqlParserPos.ZERO);
      }
      if (field instanceof Relationship) {
        if (i == path.size() - 1 && lastAlias.isPresent()) {
          this.joinInternal((Relationship) field, currentAlias, lastAlias.map(Name::getCanonical));
        } else {
          this.join((Relationship) field);
        }
      }
    }

    return new SqlJoinDeclaration(this.sqlNode, this.trailingCondition);
  }
}