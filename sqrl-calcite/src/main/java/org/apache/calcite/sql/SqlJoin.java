/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import java.util.function.UnaryOperator;
import lombok.Getter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import org.apache.flink.calcite.shaded.org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Parse tree node representing a {@code JOIN} clause.
 */
// Sqrl: Add modifier
public class SqlJoin extends SqlCall {

  static final SqlJoinOperator COMMA_OPERATOR =
      new SqlJoinOperator("COMMA-JOIN", 16);
  public static final SqlJoinOperator OPERATOR =
      new SqlJoinOperator("JOIN", 18);

  SqlNode left;

  /**
   * Operand says whether this is a natural join. Must be constant TRUE or FALSE.
   */
  SqlLiteral natural;

  /**
   * Value must be a {@link SqlLiteral}, one of the integer codes for {@link JoinType}.
   */
  SqlLiteral joinType;
  SqlNode right;

  /**
   * Value must be a {@link SqlLiteral}, one of the integer codes for {@link JoinConditionType}.
   */
  SqlLiteral conditionType;
  SqlNode condition;
  //Sqrl: add modifier
  @Getter
  SqlLiteral modifier;

  //~ Constructors -----------------------------------------------------------

  public SqlJoin(SqlParserPos pos, SqlNode left, SqlLiteral natural,
      SqlLiteral joinType, SqlNode right, SqlLiteral conditionType,
      SqlNode condition) {
    this(pos, left, natural, convertType(joinType), right, conditionType, condition, convertModifier(joinType));
  }

  private static SqlLiteral convertModifier(SqlLiteral joinType) {
    JoinType joinTypeValue = (JoinType) joinType.getValue();
    switch (joinTypeValue) {
      case INNER_TEMPORAL:
      case LEFT_TEMPORAL:
      case RIGHT_TEMPORAL:
        return JoinModifier.TEMPORAL.symbol(joinType.getParserPosition());
      case INNER_INTERVAL:
      case LEFT_INTERVAL:
      case RIGHT_INTERVAL:
        return JoinModifier.INTERVAL.symbol(joinType.getParserPosition());
      case INNER_DEFAULT:
      case LEFT_DEFAULT:
      case RIGHT_DEFAULT:
        return JoinModifier.DEFAULT.symbol(joinType.getParserPosition());
      case LEFT_OUTER:
      case RIGHT_OUTER:
        return JoinModifier.OUTER.symbol(joinType.getParserPosition());
      default:
        return JoinModifier.NONE.symbol(joinType.getParserPosition());
    }
  }

  private static SqlLiteral convertType(SqlLiteral joinType) {
    JoinType joinTypeValue = (JoinType) joinType.getValue();
    switch (joinTypeValue) {
      case INNER_TEMPORAL:
      case INNER_INTERVAL:
      case INNER_DEFAULT:
        return JoinType.INNER.symbol(joinType.getParserPosition());
      case LEFT_OUTER:
      case LEFT_TEMPORAL:
      case LEFT_INTERVAL:
      case LEFT_DEFAULT:
        return JoinType.LEFT.symbol(joinType.getParserPosition());
      case RIGHT_OUTER:
      case RIGHT_TEMPORAL:
      case RIGHT_INTERVAL:
      case RIGHT_DEFAULT:
        return JoinType.RIGHT.symbol(joinType.getParserPosition());
      default:
        return joinType;
    }
  }

  public SqlJoin(SqlParserPos pos, SqlNode left, SqlLiteral natural,
      SqlLiteral joinType, SqlNode right, SqlLiteral conditionType,
      SqlNode condition, SqlLiteral modifier) {
    super(pos);
    this.left = left;
    this.natural = Objects.requireNonNull(natural);
    this.joinType = Objects.requireNonNull(joinType);
    this.right = right;
    this.conditionType = Objects.requireNonNull(conditionType);
    this.condition = condition;
    this.modifier = modifier;

    Preconditions.checkArgument(natural.getTypeName() == SqlTypeName.BOOLEAN);
    Objects.requireNonNull(conditionType.symbolValue(JoinConditionType.class));
    Objects.requireNonNull(joinType.symbolValue(JoinType.class));
  }

  //~ Methods ----------------------------------------------------------------

  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public SqlKind getKind() {
    return SqlKind.JOIN;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(left, natural, joinType, right,
        conditionType, condition, modifier);
  }

  @Override
  public void setOperand(int i, SqlNode operand) {
    switch (i) {
      case 0:
        left = operand;
        break;
      case 1:
        natural = (SqlLiteral) operand;
        break;
      case 2:
        joinType = (SqlLiteral) operand;
        break;
      case 3:
        right = operand;
        break;
      case 4:
        conditionType = (SqlLiteral) operand;
        break;
      case 5:
        condition = operand;
        break;
      case 6:
        modifier =  (SqlLiteral) operand;
        break;
      default:
        throw new AssertionError(i);
    }
  }

  public final SqlNode getCondition() {
    return condition;
  }

  /**
   * Returns a {@link JoinConditionType}, never null.
   */
  public final JoinConditionType getConditionType() {
    return conditionType.symbolValue(JoinConditionType.class);
  }

  public SqlLiteral getConditionTypeNode() {
    return conditionType;
  }

  /**
   * Returns a {@link JoinType}, never null.
   */
  public final JoinType getJoinType() {
    return joinType.symbolValue(JoinType.class);
  }

  public SqlLiteral getJoinTypeNode() {
    return joinType;
  }

  public final SqlNode getLeft() {
    return left;
  }

  public void setLeft(SqlNode left) {
    this.left = left;
  }

  public final boolean isNatural() {
    return natural.booleanValue();
  }

  public final SqlLiteral isNaturalNode() {
    return natural;
  }

  public final SqlNode getRight() {
    return right;
  }

  public void setRight(SqlNode right) {
    this.right = right;
  }

  /** Describes the syntax of the SQL {@code JOIN} operator.
   *
   * <p>A variant describes the comma operator, which has lower precedence.
   */
  public static class SqlJoinOperator extends SqlOperator {
    private static final SqlWriter.FrameType FRAME_TYPE =
        SqlWriter.FrameTypeEnum.create("USING");

    //~ Constructors -----------------------------------------------------------

    private SqlJoinOperator(String name, int prec) {
      super(name, SqlKind.JOIN, prec, true, null, null, null);
    }

    //~ Methods ----------------------------------------------------------------

    @Override public SqlSyntax getSyntax() {
      return SqlSyntax.SPECIAL;
    }

    @SuppressWarnings("argument.type.incompatible")
    @Override public SqlCall createCall(
        @Nullable SqlLiteral functionQualifier,
        SqlParserPos pos,
        @Nullable SqlNode... operands) {
      assert functionQualifier == null;
      return new SqlJoin(pos, operands[0], (SqlLiteral) operands[1],
          (SqlLiteral) operands[2], operands[3], (SqlLiteral) operands[4],
          operands[5],  (SqlLiteral) operands[6]);
    }

    @Override public void unparse(
        SqlWriter writer,
        SqlCall call,
        int leftPrec,
        int rightPrec) {
      final SqlJoin join = (SqlJoin) call;

      join.left.unparse(
          writer,
          leftPrec,
          getLeftPrec());
      switch (join.getJoinType()) {
        case COMMA:
          writer.sep(",", true);
          break;
        case CROSS:
          writer.sep(join.isNatural() ? "NATURAL CROSS JOIN" : "CROSS JOIN");
          break;
        case FULL:
          writer.sep(join.isNatural() ? "NATURAL FULL JOIN" : "FULL JOIN");
          break;
        case INNER:
          writer.sep(join.isNatural() ? "NATURAL INNER JOIN" : "INNER JOIN");
          break;
        case LEFT:
          writer.sep(join.isNatural() ? "NATURAL LEFT JOIN" : "LEFT JOIN");
          break;
        case LEFT_SEMI_JOIN:
          writer.sep(join.isNatural() ? "NATURAL LEFT SEMI JOIN"
              : "LEFT SEMI JOIN");
          break;
        case RIGHT:
          writer.sep(join.isNatural() ? "NATURAL RIGHT JOIN" : "RIGHT JOIN");
          break;
        default:
          throw Util.unexpected(join.getJoinType());
      }
      join.right.unparse(writer, getRightPrec(), rightPrec);
      SqlNode joinCondition = join.condition;
      if (joinCondition != null) {
        switch (join.getConditionType()) {
          case USING:
            // No need for an extra pair of parens -- the condition is a
            // list. The result is something like "USING (deptno, gender)".
            writer.keyword("USING");
            assert joinCondition instanceof SqlNodeList
                : "joinCondition should be SqlNodeList, got " + joinCondition;
            final SqlWriter.Frame frame =
                writer.startList(FRAME_TYPE, "(", ")");
            joinCondition.unparse(writer, 0, 0);
            writer.endList(frame);
            break;

          case ON:
            writer.keyword("ON");
            joinCondition.unparse(writer, leftPrec, rightPrec);
            break;

          default:
            throw Util.unexpected(join.getConditionType());
        }
      }
    }
  }

  @Override public SqlString toSqlString(UnaryOperator<SqlWriterConfig> transform) {
    SqlNode selectWrapper =
        new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY,
            SqlNodeList.SINGLETON_STAR, this, null, null, null,
            SqlNodeList.EMPTY, null, null, null, SqlNodeList.EMPTY);
    return selectWrapper.toSqlString(transform);
  }
}
