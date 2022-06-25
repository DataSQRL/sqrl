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
      case USING:
        SqlNodeList list = (SqlNodeList) condition;

        // Parser ensures that using clause is not empty.
        Preconditions.checkArgument(list.size() > 0, "Empty USING clause");
        for (SqlNode node : list) {
          SqlIdentifier id = (SqlIdentifier) node;
//          final RelDataType leftColType = validateUsingCol(id, left);
//          final RelDataType rightColType = validateUsingCol(id, right);
//          if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
//            throw newValidationError(
//                id,
//                RESOURCE.naturalOrUsingColumnNotCompatible(
//                    id.getSimple(),
//                    leftColType.toString(),
//                    rightColType.toString()));
//          }
//          checkRollUpInUsing(id, left, scope);
//          checkRollUpInUsing(id, right, scope);
        }
        break;
      default:
        throw Util.unexpected(conditionType);
    }

    // Validate NATURAL.
    if (natural) {
      if (condition != null) {
        throw newValidationError(condition, RESOURCE.naturalDisallowsOnOrUsing());
      }

      // Join on fields that occur exactly once on each side. Ignore
      // fields that occur more than once on either side.
      final RelDataType leftRowType = getNamespace(left).getRowType();
      final RelDataType rightRowType = getNamespace(right).getRowType();
      final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
      List<String> naturalColumnNames =
          SqlValidatorUtil.deriveNaturalJoinColumnList(
              nameMatcher, leftRowType, rightRowType);

      // Check compatibility of the chosen columns.
      for (String name : naturalColumnNames) {
        final RelDataType leftColType = nameMatcher.field(leftRowType, name).getType();
        final RelDataType rightColType = nameMatcher.field(rightRowType, name).getType();
        if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
          throw newValidationError(
              join,
              RESOURCE.naturalOrUsingColumnNotCompatible(
                  name, leftColType.toString(), rightColType.toString()));
        }
      }
    }

    // Which join types require/allow a ON/USING condition, or allow
    // a NATURAL keyword?
    switch (joinType) {
      case LEFT_SEMI_JOIN:
//        if (!this.config.sqlConformance().isLiberal()) {
//          throw newValidationError(
//              join.getJoinTypeNode(),
//              RESOURCE.dialectDoesNotSupportFeature("LEFT SEMI JOIN"));
//        }
        // fall through
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
