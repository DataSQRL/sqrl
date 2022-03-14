package ai.dataeng.sqml.planner;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import lombok.Value;
import org.apache.calcite.schema.SqrlCalciteTable;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.OperatorTable;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.SqlValidator;

/**
 * Expands join paths:
 * _.orders.entries => JOIN _ JOIN _.orders o => o.entries;
 *
 * Adds grouping keys:
 * Orders.entries.qty := SELECT sum(quantity) FROM _; =>
 *   SELECT order_id, sum(quantity) FROM _ GROUP BY order_id;
 *
 * Note: This does not rewrite DISTINCTs.
 */
public class SqrlMacroExpand {

  private final SqlValidator validator;

  public SqrlMacroExpand(SqlValidator validator) {
    this.validator = validator;
  }

  public void expand(SqlSelect select) {
    Scope scope = expandSelectBody(select.getFrom());

    SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.addAll(scope.getPrimaryKeyPullup());
    selectList.addAll(select.getSelectList().getList());
    select.setSelectList(selectList);

    if (select.getGroup() != null) {
      SqlNodeList newNodes = new SqlNodeList(SqlParserPos.ZERO);
      newNodes.addAll(scope.getPrimaryKeyPullup());
      newNodes.addAll((select.getGroup()).getList());
      select.setGroupBy(newNodes);
    } else {
      if (hasAgg(selectList)) {
        select.setGroupBy(new SqlNodeList(scope.getPrimaryKeyPullup(), SqlParserPos.ZERO));
      }
    }
    if (select.getOrderList() != null) {
      SqlNodeList newNodes = new SqlNodeList(SqlParserPos.ZERO);
      newNodes.addAll(scope.getPrimaryKeyPullup());
      newNodes.addAll(select.getOrderList().getList());
      select.setOrderBy(newNodes);
    }

    select.setFrom(scope.from);
  }

  public class AggChecker extends SqlBasicVisitor {

    boolean hasAgg = false;

    @Override
    public Object visit(SqlCall call) {
      if (call.getOperator().isAggregator()) {
        hasAgg = true;
      }
      return super.visit(call);
    }

    public boolean hasAgg() {
      return hasAgg;
    }
  }

  private boolean hasAgg(SqlNodeList selectList) {
    AggChecker aggChecker = new AggChecker();
    selectList.accept(aggChecker);
    return aggChecker.hasAgg();
  }

  private Scope expandSelectBody(SqlNode from) {
    if (from instanceof SqlJoin) {
      return expandJoin((SqlJoin) from);
    } else if (from instanceof SqlCall) {
      return expandTable((SqlCall) from);
    } else {
      throw new RuntimeException();
    }
  }

  private Scope expandJoin(SqlJoin join) {
    Scope scopeLeft = expandSelectBody(join.getLeft());
    Scope scopeRight = expandSelectBody(join.getRight());

    join.setLeft(scopeLeft.from);
    join.setRight(scopeRight.from);

    List<SqlNode> pkPullup = new ArrayList<>(scopeLeft.getPrimaryKeyPullup());
    pkPullup.addAll(scopeRight.getPrimaryKeyPullup());
    Preconditions.checkState(pkPullup.size() == Math.max(
        scopeLeft.getPrimaryKeyPullup().size(),
        scopeRight.getPrimaryKeyPullup().size()), "Table cannot have more than one context key");

    return new Scope(
        join,
        pkPullup,
        scopeRight.getAlias()
    );
  }

  private Scope expandTable(SqlCall call) {
    if (call.getOperator() != OperatorTable.AS) {
      throw new RuntimeException(String.format("Table unexpected: %s", call));
    }

    SqlNode tableIdent = call.getOperandList().get(0);
    Preconditions.checkState(tableIdent instanceof SqlIdentifier);
    IdentifierNamespace ns = (IdentifierNamespace) validator.getNamespace(tableIdent);
    SqrlCalciteTable tbl = (SqrlCalciteTable) ns.getRowType();
    SqlIdentifier tableAlias = (SqlIdentifier) call.getOperandList().get(1);
    String tableAliasName = tableAlias.names.get(0);

    AliasGenerator gen = new AliasGenerator();

    List<SqlNode> primaryKeyPullup = new ArrayList<>();
    if (tbl.isContext()) {
      for (Column pk : tbl.getBaseTable().getPrimaryKeys()) {
        primaryKeyPullup.add(new SqlIdentifier(List.of("_", pk.getId()),
            SqlParserPos.ZERO));
      }
    }

    if (tbl.getFieldPath().isPresent()) {
      List<Field> fields = tbl.getFieldPath().get().getFields();
      String currentAlias = tbl.isContext() ? "_" : gen.nextAlias();
      SqlNode current = alias(fields.get(0).getTable().getPath().toString(), currentAlias);

      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        Relationship rel = (Relationship) field;
        String nextAlias = gen.nextAlias();
        if (i == fields.size() - 1) {
          nextAlias = tableAliasName;
        }

        current = join(
            current,
            alias(currentAlias + "." + rel.getName(), nextAlias)
        );

        currentAlias = nextAlias;
      }
      return new Scope(current, primaryKeyPullup, currentAlias);
    } else {
      return new Scope(call, primaryKeyPullup, tableAliasName);
    }
  }

  private SqlNode alias(String table, String alias) {
    return new SqlBasicCall(OperatorTable.AS,
        createIdent(table, alias),
        SqlParserPos.ZERO
    );
  }

  private SqlNode join(SqlNode left, SqlNode right) {
    return new SqlJoin(SqlParserPos.ZERO,
        left,
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        SqlLiteral.createSymbol(JoinType.INNER, SqlParserPos.ZERO),
        right,
        SqlLiteral.createSymbol(JoinConditionType.NONE, SqlParserPos.ZERO),
        null
    );
  }

  private SqlNode[] createIdent(String table, String alias) {
    return new SqlIdentifier[]{
        new SqlIdentifier(table, SqlParserPos.ZERO),
        new SqlIdentifier(alias, SqlParserPos.ZERO)
    };
  }

  @Value
  class Scope {
    public SqlNode from;
    public List<SqlNode> primaryKeyPullup;
    public String alias;
  }
}