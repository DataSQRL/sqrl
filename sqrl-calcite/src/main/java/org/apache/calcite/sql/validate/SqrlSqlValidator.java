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

import static org.apache.calcite.util.Static.RESOURCE;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Util;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;

import com.datasqrl.util.ReflectionUtil;
import com.google.common.base.Preconditions;

public class SqrlSqlValidator extends FlinkCalciteSqlValidator {

  private final SqlValidatorCatalogReader catalogReader;

  public SqrlSqlValidator(SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader,
      RelDataTypeFactory typeFactory, Config config) {
    super(opTab, catalogReader, typeFactory, config);
    this.catalogReader = catalogReader;
  }

  @Override
protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
    var left = join.getLeft();
    var right = join.getRight();
    var condition = join.getCondition();
    var natural = join.isNatural();
    final var joinType = join.getJoinType();
    final var conditionType = join.getConditionType();
    final var joinScope = scopes.get(join);
    validateFrom(left, unknownType, joinScope);
    validateFrom(right, unknownType, joinScope);

    // Validate condition.
    switch (conditionType) {
      case NONE:
        Preconditions.checkArgument(condition == null);
        break;
      case ON:
        Preconditions.checkArgument(condition != null);
        var expandedCondition = expand(condition, joinScope);
        join.setOperand(5, expandedCondition);
        condition = join.getCondition();
        validateWhereOrOn(joinScope, condition, "ON");
//        checkRollUp(null, join, condition, joinScope, "ON");
        break;
      case USING:
        var list = (SqlNodeList) condition;

         //Parser ensures that using clause is not empty.
        Preconditions.checkArgument(list.size() > 0, "Empty USING clause");
        for (SqlNode node : list) {
          var id = (SqlIdentifier) node;
          final var leftColType = (RelDataType)ReflectionUtil.invokeSuperPrivateMethod(this,
              "validateUsingCol", List.of(SqlIdentifier.class, SqlNode.class), id, left);
          final var rightColType = (RelDataType)ReflectionUtil.invokeSuperPrivateMethod(this,
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
      final var leftRowType = getNamespace(left).getRowType();
      final var rightRowType = getNamespace(right).getRowType();
      final var nameMatcher = catalogReader.nameMatcher();
      var naturalColumnNames =
          SqlValidatorUtil.deriveNaturalJoinColumnList(
              nameMatcher, leftRowType, rightRowType);

      // Check compatibility of the chosen columns.
      for (String name : naturalColumnNames) {
        final var leftColType = nameMatcher.field(leftRowType, name).getType();
        final var rightColType = nameMatcher.field(rightRowType, name).getType();
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
      case COMMA:
      case CROSS:
        if ((condition != null) || natural) {
          throw newValidationError(
              join.getConditionTypeNode(), RESOURCE.crossJoinDisallowsCondition());
        }
        break;
      default:
        throw Util.unexpected(joinType);
    }
  }

  @Override
public SqlNode getAggregate(SqlSelect select) {
    return super.getAggregate(select);
  }
}
