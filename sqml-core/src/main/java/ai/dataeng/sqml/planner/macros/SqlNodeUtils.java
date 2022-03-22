package ai.dataeng.sqml.planner.macros;

import ai.dataeng.sqml.planner.AliasGenerator;
import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.Relationship;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.SqrlCalciteTable;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlOperatorTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.commons.lang3.tuple.Pair;

public class SqlNodeUtils {

  public static Pair<Map<Column, String>, SqlNode> parentChildJoin(Relationship col) {
    AliasGenerator gen = new AliasGenerator();
    String lhs = gen.nextTableAlias();
    String rhs = gen.nextTableAlias();

    List<SqlNode> conditions = new ArrayList();
    Map<Column, String> map = new HashMap<>();
    for (Column column : col.getTable().getPrimaryKeys()) {
      Field fk = col.getToTable().getField(column.getName());

      SqlNode condition = SqlNodeUtils.eq(SqlNodeUtils.ident(lhs, column.getId().toString()),
          SqlNodeUtils.ident(rhs, fk.getId().toString()));
      conditions.add(condition);
      map.put(column, column.getId().toString());
      map.put((Column)fk, fk.getId().toString());
    }

    SqlNode condition = SqlNodeUtils.and(conditions);
    //SELECT * FROM tableA AS lhs JOIN tableB AS rhs ON eq;
    SqlNode join = join(
        JoinType.INNER, toCall(col.getTable().getId().toString(), lhs),
        toCall(col.getToTable().getId().toString(), rhs), condition);
    SqlSelect select = select(selectAllList(rhs), join);

    return Pair.of(map, select);
  }
  public static Pair<Map<Column, String>, SqlNode> childParentJoin(Relationship col) {
    AliasGenerator gen = new AliasGenerator();
    String lhs = gen.nextTableAlias();
    String rhs = gen.nextTableAlias();

    List<SqlNode> conditions = new ArrayList();
    Map<Column, String> map = new HashMap<>();

    for (Column column : col.getToTable().getPrimaryKeys()) {
      Field fk = col.getTable().getField(column.getName());

      SqlNode condition = SqlNodeUtils.eq(SqlNodeUtils.ident(lhs, column.getId().toString()),
          SqlNodeUtils.ident(rhs, fk.getId().toString()));
      conditions.add(condition);
      map.put(column, column.getId().toString());
      map.put((Column)fk, fk.getId().toString());
    }
    Preconditions.checkState(!conditions.isEmpty(), "Could not find FK");

    SqlNode condition = SqlNodeUtils.and(conditions);
    //SELECT * FROM tableA AS lhs JOIN tableB AS rhs ON eq;
    SqlNode join = join(
        JoinType.INNER, toCall(col.getTable().getId().toString(), lhs),
        toCall(col.getToTable().getId().toString(), rhs), condition);
    SqlSelect select = select(selectAllList(rhs), join);

    return Pair.of(map, select);
  }


  public static SqlSelect select(SqlNodeList selectList, SqlNode from) {
    return new SqlSelect(SqlParserPos.ZERO, null, selectList, from, null, null, null, null, null,
        null, null, null);
  }

  public static SqlNodeList selectAllList(String name) {
    return new SqlNodeList(List.of(new SqlIdentifier(List.of(name, ""), SqlParserPos.ZERO)),
        SqlParserPos.ZERO);
  }

  public static SqlIdentifier ident(String alias, String name) {
    return new SqlIdentifier(List.of(alias, name), SqlParserPos.ZERO);
  }

  public static SqlNode eq(SqlNode ident, SqlNode ident1) {
    SqlNode[] condition = new SqlNode[]{ident, ident1};
    return new SqlBasicCall(SqrlOperatorTable.EQUALS, condition, SqlParserPos.ZERO);
  }

  public static SqlNode and(List<SqlNode> conditions) {
    Preconditions.checkState(!conditions.isEmpty(), "Conditions are empty");
    if (conditions.size() == 1) {
      return conditions.get(0);
    }

    SqlNode[] condition = conditions.toArray(SqlNode[]::new);
    return new SqlBasicCall(SqrlOperatorTable.AND, condition, SqlParserPos.ZERO);
  }


  public static SqrlCalciteTable getTableForCall(SqlValidator validator, SqlBasicCall call) {
    SqlNode tableIdent = call.getOperandList().get(0);
    Preconditions.checkState(tableIdent instanceof SqlIdentifier);
    IdentifierNamespace ns = (IdentifierNamespace) validator.getNamespace(tableIdent);
    Preconditions.checkNotNull(ns);
    Preconditions.checkState(ns.getRowType() instanceof SqrlCalciteTable);
    return (SqrlCalciteTable) ns.getRowType();
  }

  public static SqlNode toCall(String table, String alias) {
    return new SqlBasicCall(SqrlOperatorTable.AS,
        createIdent(table, alias),
        SqlParserPos.ZERO
    );
  }
  public static SqlBasicCall toCall(SqlNode table, String alias) {
    return new SqlBasicCall(SqrlOperatorTable.AS,
        createIdent(table, alias),
        SqlParserPos.ZERO
    );
  }

  public static SqlNode[] createIdent(SqlNode node, String alias) {
    return new SqlNode[]{
        node,
        new SqlIdentifier(alias, SqlParserPos.ZERO)
    };
  }

  public static SqlNode[] createIdent(String table, String alias) {
    return new SqlIdentifier[]{
        new SqlIdentifier(table, SqlParserPos.ZERO),
        new SqlIdentifier(alias, SqlParserPos.ZERO)
    };
  }

  public static SqlJoin join(JoinType joinType, SqlNode left, SqlNode right, SqlNode condition) {
    return new SqlJoin(SqlParserPos.ZERO,
        left,
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        SqlLiteral.createSymbol(joinType, SqlParserPos.ZERO),
        right,
        SqlLiteral.createSymbol(condition == null ? JoinConditionType.NONE : JoinConditionType.ON,
            SqlParserPos.ZERO),
        condition
    );
  }

  public static SqlBasicCall getSiblingCall(SqlNode node) {
    while (node instanceof SqlJoin) {
      node = ((SqlJoin) node).getRight();
    }

    return unwrapCall(node);
  }

  public static SqlBasicCall unwrapCall(SqlNode node) {
    Preconditions.checkState(node instanceof SqlBasicCall);
    SqlBasicCall call = (SqlBasicCall) node;
    Preconditions.checkState(call.getOperator() == SqrlOperatorTable.AS);
    return call;
  }

}
