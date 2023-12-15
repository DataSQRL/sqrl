/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql.validate;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.util.Static.RESOURCE;

import com.datasqrl.util.ReflectionUtil;
import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;
import org.apache.flink.table.types.logical.DecimalType;

public class SqrlSqlValidator extends FlinkCalciteSqlValidator {
  // Enables CallContext#getOutputDataType() when validating SQL expressions.
  private SqlNode sqlNodeForExpectedOutputType;
  private RelDataType expectedOutputType;

  public SqrlSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
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

         //Parser ensures that using clause is not empty.
        Preconditions.checkArgument(list.size() > 0, "Empty USING clause");
        for (SqlNode node : list) {
          SqlIdentifier id = (SqlIdentifier) node;
          final RelDataType leftColType = (RelDataType)ReflectionUtil.invokeSuperPrivateMethod(this,
              "validateUsingCol", List.of(SqlIdentifier.class, SqlNode.class), id, left);
          final RelDataType rightColType = (RelDataType)ReflectionUtil.invokeSuperPrivateMethod(this,
              "validateUsingCol", List.of(SqlIdentifier.class, SqlNode.class), id, right);
          if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
            throw newValidationError(
                id,
                RESOURCE.naturalOrUsingColumnNotCompatible(
                    id.getSimple(),
                    leftColType.toString(),
                    rightColType.toString()));
          }
          ReflectionUtil.invokeSuperPrivateMethod(this,
              "checkRollUpInUsing", List.of(SqlIdentifier.class, SqlNode.class, SqlValidatorScope.class),
              id, left, scope);
          ReflectionUtil.invokeSuperPrivateMethod(this,
              "checkRollUpInUsing", List.of(SqlIdentifier.class, SqlNode.class, SqlValidatorScope.class),
              id, right, scope);
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
      case INNER:
      case LEFT:
      case RIGHT:
      case FULL:

        if ((condition == null) && !natural) {
          throw newValidationError(join, RESOURCE.joinRequiresCondition());
        }
        break;
      // Custom SQRL join types
      case DEFAULT:
      case LEFT_DEFAULT:
      case RIGHT_DEFAULT:
      case TEMPORAL:
      case RIGHT_TEMPORAL:
      case LEFT_TEMPORAL:
      case INTERVAL:
      case LEFT_INTERVAL:
      case RIGHT_INTERVAL:
      case IMPLICIT:
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

  public void setExpectedOutputType(SqlNode sqlNode, RelDataType expectedOutputType) {
    this.sqlNodeForExpectedOutputType = sqlNode;
    this.expectedOutputType = expectedOutputType;
  }

  public Optional<RelDataType> getExpectedOutputType(SqlNode sqlNode) {
    if (sqlNode == sqlNodeForExpectedOutputType) {
      return Optional.of(expectedOutputType);
    }
    return Optional.empty();
  }

  @Override
  public void validateLiteral(SqlLiteral literal) {
    if (literal.getTypeName() == DECIMAL) {
      final BigDecimal decimal = literal.getValueAs(BigDecimal.class);
      if (decimal.precision() > DecimalType.MAX_PRECISION) {
        throw newValidationError(
            literal, Static.RESOURCE.numberLiteralOutOfRange(decimal.toString()));
      }
    }
    super.validateLiteral(literal);
  }

  @Override
  public void validateColumnListParams(
      SqlFunction function, List<RelDataType> argTypes, List<SqlNode> operands) {
    // we don't support column lists and translate them into the unknown type in the type
    // factory,
    // this makes it possible to ignore them in the validator and fall back to regular row types
    // see also SqlFunction#deriveType
  }

  public SqlNode getAggregate(SqlSelect select) {
    return super.getAggregate(select);
  }
}
