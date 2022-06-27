package org.apache.calcite.sql.validate;

import static org.apache.calcite.util.Static.RESOURCE;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Util;

public class SqrlValidator extends SqlValidatorImpl {

  /**
   * Creates a validator.
   *
   * @param opTab         Operator table
   * @param catalogReader Catalog reader
   * @param typeFactory   Type factory
   * @param config        Config
   */
  public SqrlValidator(SqlOperatorTable opTab,
      SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory, Config config) {
    super(opTab, catalogReader, typeFactory, config);
  }


  protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    SqlNode condition = join.getCondition();
    boolean natural = join.isNatural();
    final JoinType joinType = join.getJoinType();
    final JoinConditionType conditionType = join.getConditionType();
    final SqlValidatorScope joinScope = scopes.get(join);
    validateFrom(left, unknownType, joinScope);
    validateFrom(right, unknownType, joinScope);

    // Validate condition.
    switch (conditionType) {
      case NONE:
        Preconditions.checkArgument(condition == null);
        break;
      case ON:
        Preconditions.checkArgument(condition != null);
        SqlNode expandedCondition = expand(condition, joinScope);
        join.setOperand(5, expandedCondition);
        condition = join.getCondition();
        validateWhereOrOn(joinScope, condition, "ON");
//        checkRollUp(null, join, condition, joinScope, "ON");
        break;
      default:
        throw Util.unexpected(conditionType);
    }

    // Validate NATURAL.
    if (natural) {
      throw Util.unexpected(conditionType);
    }

    // Which join types require/allow a ON/USING condition, or allow
    // a NATURAL keyword?
    switch (joinType) {
      case INNER:
      case LEFT:
      case RIGHT:
      case FULL:
      case TEMPORAL:
        if ((condition == null) && !natural) {
          throw newValidationError(join, RESOURCE.joinRequiresCondition());
        }
        break;
      case COMMA:
      case CROSS:
        if (condition != null) {
          throw newValidationError(
              join.getConditionTypeNode(), RESOURCE.crossJoinDisallowsCondition());
        }
        if (natural) {
          throw newValidationError(
              join.getConditionTypeNode(), RESOURCE.crossJoinDisallowsCondition());
        }
        break;
      default:
        throw Util.unexpected(joinType);
    }
  }
}
