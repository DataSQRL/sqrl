package org.apache.calcite.sql.validate;

import static org.apache.calcite.util.Static.RESOURCE;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.prepare.SqrlCalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Util;

/**
 * Changes some validation rules for SQRL
 */
public class SqrlValidator extends SqlValidatorImpl {

  private final SqrlCalciteCatalogReader sqrlCatalogReader;

  /**
   * Creates a validator.
   *
   * @param opTab         Operator table
   * @param catalogReader Catalog reader
   * @param typeFactory   Type factory
   * @param config        Config
   */
  public SqrlValidator(SqlOperatorTable opTab,
      SqrlCalciteCatalogReader catalogReader,
      RelDataTypeFactory typeFactory,
      Config config) {
    super(opTab, catalogReader, typeFactory, config);
    this.sqrlCatalogReader = catalogReader;
  }

  /**
   * Changes:
   *  - Allows JOIN without ON
   */
  @Override
  protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    SqlNode condition = join.getCondition();
    boolean natural = join.isNatural();
    final JoinType joinType = join.getJoinType();
    final JoinConditionType conditionType = join.getConditionType();
    final SqlValidatorScope joinScope = scopes.get(join);
    if (scope instanceof SelectScope) {
      sqrlCatalogReader.setScope((SelectScope) scope);
    }
    validateFrom(left, unknownType, joinScope);
    validateFrom(right, unknownType, joinScope);

    // Validate condition.
    switch (conditionType) {
      case USING:
        break;
      case NONE:
        //For SQRL: It is okay to have NONE. We will expand the condition later.
        Preconditions.checkArgument(condition == null);
        break;
      case ON:
        Preconditions.checkArgument(condition != null);
        SqlNode expandedCondition = expand(condition, joinScope);
        join.setOperand(5, expandedCondition);
        condition = join.getCondition();
        validateWhereOrOn(joinScope, condition, "ON");
        break;
      default:
        throw Util.unexpected(conditionType);
    }

    // Which join types require/allow a ON/USING condition, or allow
    // a NATURAL keyword?
    switch (joinType) {
      case INNER:
      case LEFT:
      case RIGHT:
      case FULL:
//        if ((condition == null) && !natural) {
//          throw newValidationError(join, RESOURCE.joinRequiresCondition());
//        }
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
