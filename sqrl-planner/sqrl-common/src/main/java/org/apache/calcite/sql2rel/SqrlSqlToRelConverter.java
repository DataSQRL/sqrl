/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql2rel;

import static com.datasqrl.util.ReflectionUtil.invokeSuperPrivateMethod;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.DelegatingScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

@SuppressWarnings("UnstableApiUsage")
public class SqrlSqlToRelConverter extends SqlToRelConverter {

  public SqrlSqlToRelConverter(ViewExpander viewExpander, SqlValidator validator,
      CatalogReader catalogReader, RelOptCluster cluster,
      SqlRexConvertletTable convertletTable, Config config) {
    super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
  }

  protected void convertFrom(Blackboard bb, SqlNode from, List<String> fieldNames) {
    if (from == null) {
      bb.setRoot(LogicalValues.createOneRow(cluster), false);
      return;
    }

    switch (from.getKind()) {
      case JOIN:
        convertJoin(bb, (SqlJoin) from);
        return;
    }

    super.convertFrom(bb, from, fieldNames);
  }

  private void convertJoin(Blackboard bb, SqlJoin join) {
    final SqlValidatorScope scope = validator.getJoinScope(join);
    final Blackboard fromBlackboard = createBlackboard(scope, null, false);
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    final SqlValidatorScope leftScope =
        Util.first(validator.getJoinScope(left), ((DelegatingScope) bb.scope).getParent());
    final Blackboard leftBlackboard = createBlackboard(leftScope, null, false);
    final SqlValidatorScope rightScope =
        Util.first(validator.getJoinScope(right), ((DelegatingScope) bb.scope).getParent());
    final Blackboard rightBlackboard = createBlackboard(rightScope, null, false);
    convertFrom(leftBlackboard, left);
    final RelNode leftRel = leftBlackboard.root;
    convertFrom(rightBlackboard, right);
    final RelNode tempRightRel = rightBlackboard.root;

    final JoinConditionType conditionType = join.getConditionType();
    final RexNode condition;
    final RelNode rightRel;
    if (join.isNatural()) {
      condition =
          (RexNode) invokeSuperPrivateMethod(this, "convertNaturalCondition",
              List.of(SqlValidatorNamespace.class, SqlValidatorNamespace.class),
              validator.getNamespace(left), validator.getNamespace(right));
      rightRel = tempRightRel;
    } else {
      switch (conditionType) {
        case NONE:
          condition = rexBuilder.makeLiteral(true);
          rightRel = tempRightRel;
          break;
        case USING:
          condition =
              (RexNode) invokeSuperPrivateMethod(this, "convertUsingCondition",
                  List.of(SqlJoin.class, SqlValidatorNamespace.class, SqlValidatorNamespace.class),
                  join,
                  validator.getNamespace(left),
                  validator.getNamespace(right));
          rightRel = tempRightRel;
          break;
        case ON:
          Pair<RexNode, RelNode> conditionAndRightNode =
              (Pair<RexNode, RelNode>) invokeSuperPrivateMethod(this, "convertOnCondition",
                  List.of(Blackboard.class, SqlJoin.class, RelNode.class, RelNode.class),
                  fromBlackboard, join, (RelNode)leftRel, (RelNode)tempRightRel);
          condition = conditionAndRightNode.left;
          rightRel = conditionAndRightNode.right;
          break;
        default:
          throw Util.unexpected(conditionType);
      }
    }
    final RelNode joinRel =
        createJoin(
            fromBlackboard,
            leftRel,
            rightRel,
            condition,
            convertJoinType(join.getJoinType()));
    bb.setRoot(joinRel, false);
  }

  private static JoinRelType convertJoinType(JoinType joinType) {
    switch (joinType) {
      case COMMA:
      case INNER:
      case CROSS:
      case IMPLICIT:
        return JoinRelType.INNER;
      case FULL:
        return JoinRelType.FULL;
      case LEFT:
        return JoinRelType.LEFT;
      case RIGHT:
        return JoinRelType.RIGHT;
      case TEMPORAL:
        return JoinRelType.TEMPORAL;
      case INTERVAL:
        return JoinRelType.INTERVAL;
      case DEFAULT:
        return JoinRelType.DEFAULT;
      case LEFT_DEFAULT:
        return JoinRelType.LEFT_DEFAULT;
      case LEFT_TEMPORAL:
        return JoinRelType.LEFT_TEMPORAL;
      case LEFT_INTERVAL:
        return JoinRelType.LEFT_INTERVAL;
      case RIGHT_DEFAULT:
        return JoinRelType.RIGHT_DEFAULT;
      case RIGHT_TEMPORAL:
        return JoinRelType.RIGHT_TEMPORAL;
      case RIGHT_INTERVAL:
        return JoinRelType.RIGHT_INTERVAL;
      default:
        throw Util.unexpected(joinType);
    }
  }
}
