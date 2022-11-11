package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.parse.tree.name.ReservedName;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;
import org.apache.calcite.sql.SqrlJoinPath;
import org.apache.calcite.sql.SqrlJoinSetOperation;
import org.apache.calcite.sql.SqrlJoinTerm;
import org.apache.calcite.sql.SqrlJoinTerm.SqrlJoinTermVisitor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Orders.x := SELECT * FROM Product;
 * ->
 * Orders.x := SELECT * FROM orders$1 AS _ JOIN Product;
 *
 * Does not add self table if one is already present and is properly aliased.
 * Also adds self to expressions.
 */
@AllArgsConstructor
public class AddContextTable
    extends SqlShuttle
    implements SqrlJoinTermVisitor<SqlNode, Object> {
  private final boolean hasContext;

  @Override
  public SqlNode visitJoinPath(SqrlJoinPath node, Object context) {
    //Check to see if first relation is self, if so, return
    if (isSelfTable(node.relations.get(0))) {
      return node;
    }

    List<SqlNode> relations = new ArrayList<>();
    relations.add(createSelf());
    relations.addAll(node.getRelations());
    List<SqlNode> conditions = new ArrayList<>();
    conditions.add(null);
    conditions.addAll(node.getConditions());

    return new SqrlJoinPath(
        node.getParserPosition(),
        relations,
        conditions);
  }

  private boolean isSelfTable(SqlNode node) {
    switch (node.getKind()) {
      case AS:
        SqlCall call = (SqlCall) node;
        SqlNode table = call.getOperandList().get(0);
        return isSelfTable(table);
      case IDENTIFIER:
        SqlIdentifier identifier = (SqlIdentifier) node;
        if (identifier.names.size() == 1 && identifier.names.get(0).equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
          return true;
        }
    }
    return false;
  }

  @Override
  public SqlNode visitJoinSetOperation(SqrlJoinSetOperation sqrlJoinSetOperation, Object context) {
    return null;
  }

  private SqlNode addLeftDeep(SqlJoin join) {
    return null;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    //Already exists
    if (id.names.size() == 1 && id.names.get(0).equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      return createSelf();
    }
    return addSelfLeftDeep(id);
  }

  @Override
  public SqlNode visit(SqlCall node) {
    if (!hasContext) {
      return node;
    }
    ValidateNoReservedAliases noReservedAliases = new ValidateNoReservedAliases();
    node.accept(noReservedAliases);

    switch (node.getKind()) {
      case JOIN_DECLARATION:
        SqrlJoinDeclarationSpec spec = (SqrlJoinDeclarationSpec) node;
        SqrlJoinTerm term = spec.getRelation();
        SqrlJoinTerm newTerm = (SqrlJoinTerm)term.accept(this, null);
        return new SqrlJoinDeclarationSpec(spec.getParserPosition(),
            newTerm,
            spec.orderList,
            spec.fetch,
            spec.inverse,
            spec.getLeftJoins());
      case ORDER_BY: //select, union, or join
        SqlOrderBy order = (SqlOrderBy) node;
        return new SqlOrderBy(order.getParserPosition(),
            order.query.accept(this), order.orderList, order.offset, order.fetch);
      case UNION:
        SqlBasicCall call = (SqlBasicCall)node;
        SqlNode[] operands = call.getOperandList().stream()
            .map(o->o.accept(this))
            .toArray(SqlNode[]::new);
        return new SqlBasicCall(call.getOperator(), operands, call.getParserPosition());
      case JOIN:
        //walk left deep
        SqlJoin join2 =(SqlJoin) node;
        SqlNode node2 = join2.getLeft();
        SqlNode newLeft = node2.accept(this);
        join2.setLeft(newLeft);
        return join2;
      case AS:
        //is a table
        return addSelfLeftDeep(node);
      case SELECT:
        SqlSelect select = (SqlSelect) node;
        if (select.getFrom() == null) {
          select.setFrom(createSelf());
        } else {
          select.setFrom(select.getFrom().accept(this));
        }
        return select;
    }
    return super.visit(node);
  }

  private SqlNode addSelfLeftDeep(SqlNode node) {
    return new SqlJoin(
        SqlParserPos.ZERO,
        createSelf(),
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        JoinType.DEFAULT.symbol(SqlParserPos.ZERO),
        node,
        JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
        null
    );
  }

  private SqlNode createSelf() {
    return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
        new SqlIdentifier(ReservedName.SELF_IDENTIFIER.getCanonical(), SqlParserPos.ZERO),
        new SqlIdentifier(ReservedName.SELF_IDENTIFIER.getCanonical(), SqlParserPos.ZERO));
  }

  class ValidateNoReservedAliases extends SqlBasicVisitor<SqlNode> {
    @Override
    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {
        case AS:
          validateNotReserved((SqlIdentifier)call.getOperandList().get(1));
      }
      return super.visit(call);
    }

    private void validateNotReserved(SqlIdentifier identifier) {
      Preconditions.checkState(identifier.names.size() == 1 &&
              !identifier.names.get(0).equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical()),
          "The SELF keyword is reserved, use a different alias"
      );
    }
  }
}
