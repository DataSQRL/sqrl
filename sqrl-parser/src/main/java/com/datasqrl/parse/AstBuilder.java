/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.parse;

import com.datasqrl.name.Name;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.name.NamePath;
import com.datasqrl.name.ReservedName;
import com.datasqrl.parse.SqlBaseParser.*;
import com.google.common.base.Preconditions;
import java.util.stream.Collectors;
import org.apache.calcite.sql.TableFunctionArgument;
import java.util.Locale;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Builds the abstract syntax tree for an SQRL script
 */
class AstBuilder
    extends SqlBaseBaseVisitor<SqlNode> {

  private static void check(boolean condition, String message, ParserRuleContext context) {
    if (!condition) {
      throw parseError(message, context);
    }
  }

  private static String decodeUnicodeLiteral(UnicodeStringLiteralContext context) {
    char escape;
    if (context.UESCAPE() != null) {
      String escapeString = unquote(context.STRING().getText());
      check(!escapeString.isEmpty(), "Empty Unicode escape character", context);
      check(escapeString.length() == 1, "Invalid Unicode escape character: " + escapeString,
          context);
      escape = escapeString.charAt(0);
      check(isValidUnicodeEscape(escape), "Invalid Unicode escape character: " + escapeString,
          context);
    } else {
      escape = '\\';
    }

    String rawContent = unquote(context.UNICODE_STRING().getText().substring(2));
    StringBuilder unicodeStringBuilder = new StringBuilder();
    StringBuilder escapedCharacterBuilder = new StringBuilder();
    int charactersNeeded = 0;
    UnicodeDecodeState state = UnicodeDecodeState.EMPTY;
    for (int i = 0; i < rawContent.length(); i++) {
      char ch = rawContent.charAt(i);
      switch (state) {
        case EMPTY:
          if (ch == escape) {
            state = UnicodeDecodeState.ESCAPED;
          } else {
            unicodeStringBuilder.append(ch);
          }
          break;
        case ESCAPED:
          if (ch == escape) {
            unicodeStringBuilder.append(escape);
            state = UnicodeDecodeState.EMPTY;
          } else if (ch == '+') {
            state = UnicodeDecodeState.UNICODE_SEQUENCE;
            charactersNeeded = 6;
          } else if (isHexDigit(ch)) {
            state = UnicodeDecodeState.UNICODE_SEQUENCE;
            charactersNeeded = 4;
            escapedCharacterBuilder.append(ch);
          } else {
            throw parseError("Invalid hexadecimal digit: " + ch, context);
          }
          break;
        case UNICODE_SEQUENCE:
          check(isHexDigit(ch), "Incomplete escape sequence: " + escapedCharacterBuilder,
              context);
          escapedCharacterBuilder.append(ch);
          if (charactersNeeded == escapedCharacterBuilder.length()) {
            String currentEscapedCode = escapedCharacterBuilder.toString();
            escapedCharacterBuilder.setLength(0);
            int codePoint = Integer.parseInt(currentEscapedCode, 16);
            check(Character.isValidCodePoint(codePoint),
                "Invalid escaped character: " + currentEscapedCode, context);
            if (Character.isSupplementaryCodePoint(codePoint)) {
              unicodeStringBuilder.appendCodePoint(codePoint);
            } else {
              char currentCodePoint = (char) codePoint;
              check(!Character.isSurrogate(currentCodePoint), format(
                  "Invalid escaped character: %s. Escaped character is a surrogate. Use '\\+123456' instead.",
                  currentEscapedCode), context);
              unicodeStringBuilder.append(currentCodePoint);
            }
            state = UnicodeDecodeState.EMPTY;
            charactersNeeded = -1;
          } else {
            check(charactersNeeded > escapedCharacterBuilder.length(),
                "Unexpected escape sequence length: " + escapedCharacterBuilder.length(), context);
          }
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }

    check(state == UnicodeDecodeState.EMPTY,
        "Incomplete escape sequence: " + escapedCharacterBuilder, context);
    return unicodeStringBuilder.toString();
  }

  private static String unquote(String value) {
    return value.substring(1, value.length() - 1)
        .replace("''", "'");
  }

  // ******************* statements **********************

  private static boolean isDistinct(SetQuantifierContext setQuantifier) {
    return setQuantifier != null && setQuantifier.DISTINCT() != null;
  }

  private static boolean isHexDigit(char c) {
    return ((c >= '0') && (c <= '9')) ||
        ((c >= 'A') && (c <= 'F')) ||
        ((c >= 'a') && (c <= 'f'));
  }

  private static boolean isValidUnicodeEscape(char c) {
    return c < 0x7F && c > 0x20 && !isHexDigit(c) && c != '"' && c != '+' && c != '\'';
  }

  private static Optional<String> getTextIfPresent(Token token) {
    return Optional.ofNullable(token)
        .map(Token::getText);
  }

  private static SqlOperator getArithmeticBinaryOperator(Token operator) {
    switch (operator.getType()) {
      case SqlBaseLexer.PLUS:
        return SqlStdOperatorTable.PLUS;
      case SqlBaseLexer.MINUS:
        return SqlStdOperatorTable.MINUS;
      case SqlBaseLexer.ASTERISK:
        return SqlStdOperatorTable.MULTIPLY;
      case SqlBaseLexer.SLASH:
        return SqlStdOperatorTable.DIVIDE;
      case SqlBaseLexer.PERCENT:
        return SqlStdOperatorTable.MOD;
    }

    throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
  }

  private static SqlOperator getComparisonOperator(Token symbol) {
    switch (symbol.getType()) {
      case SqlBaseLexer.EQ:
        return SqlStdOperatorTable.EQUALS;
      case SqlBaseLexer.NEQ:
        return SqlStdOperatorTable.NOT_EQUALS;
      case SqlBaseLexer.LT:
        return SqlStdOperatorTable.LESS_THAN;
      case SqlBaseLexer.LTE:
        return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
      case SqlBaseLexer.GT:
        return SqlStdOperatorTable.GREATER_THAN;
      case SqlBaseLexer.GTE:
        return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    }

    throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
  }

  private static TimeUnit getIntervalFieldType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.YEAR:
      case SqlBaseLexer.YEARS:
        return TimeUnit.YEAR;
      case SqlBaseLexer.MONTH:
      case SqlBaseLexer.MONTHS:
        return TimeUnit.MONTH;
      case SqlBaseLexer.DAY:
      case SqlBaseLexer.DAYS:
        return TimeUnit.DAY;
      case SqlBaseLexer.WEEK:
      case SqlBaseLexer.WEEKS:
        return TimeUnit.WEEK;
      case SqlBaseLexer.HOUR:
      case SqlBaseLexer.HOURS:
        return TimeUnit.HOUR;
      case SqlBaseLexer.MINUTE:
      case SqlBaseLexer.MINUTES:
        return TimeUnit.MINUTE;
      case SqlBaseLexer.SECOND:
      case SqlBaseLexer.SECONDS:
        return TimeUnit.SECOND;
    }

    throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
  }

//  private static IntervalLiteral.Sign getIntervalSign(Token token) {
//    switch (token.getType()) {
//      case SqlBaseLexer.MINUS:
//        return IntervalLiteral.Sign.NEGATIVE;
//      case SqlBaseLexer.PLUS:
//        return IntervalLiteral.Sign.POSITIVE;
//    }
//
//    throw new IllegalArgumentException("Unsupported sign: " + token.getText());
//  }

  private static SqlOperator getLogicalBinaryOperator(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.AND:
        return SqlStdOperatorTable.AND;
      case SqlBaseLexer.OR:
        return SqlStdOperatorTable.OR;
    }

    throw new IllegalArgumentException("Unsupported operator: " + token.getText());
  }

  private static Optional<SqlPostfixOperator> getOrderingType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.ASC:
        return Optional.empty();
      case SqlBaseLexer.DESC:
        return Optional.of(SqlStdOperatorTable.DESC);
    }

    throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
  }

  public static SqlParserPos getLocation(TerminalNode terminalNode) {
    requireNonNull(terminalNode, "terminalNode is null");
    return getLocation(terminalNode.getSymbol());
  }

  public static SqlParserPos getLocation(ParserRuleContext parserRuleContext) {
    requireNonNull(parserRuleContext, "parserRuleContext is null");
    return getLocation(parserRuleContext.getStart());
  }

  public static SqlParserPos getLocation(Token token) {
    requireNonNull(token, "token is null");
    return new SqlParserPos(token.getLine(), token.getCharPositionInLine());
  }

  private static ParsingException parseError(String message, ParserRuleContext context) {
    return new ParsingException(message, null, context.getStart().getLine(),
        context.getStart().getCharPositionInLine());
  }

  @Override
  public SqlNode visitSingleStatement(SingleStatementContext context) {
    return visit(context.statement());
  }

  @Override
  public SqlNode visitQuery(QueryContext context) {
    return visit(context.queryNoWith());
//    Query body = (Query) visit(context.queryNoWith());
//
//    return new Query(
//        getLocation(context),
//        body.getQueryBody(),
//        body.getOrderBy(),
//        body.getLimit());
  }

  @Override
  public SqlNode visitQueryNoWith(QueryNoWithContext context) {
    SqlCall term = (SqlCall) visit(context.queryTerm());

    SqlNodeList orderBy = null;
    if (context.ORDER() != null) {
      List<SqlNode> order = visit(context.sortItem(), SqlNode.class);
      orderBy = new SqlNodeList(order, getLocation(context.ORDER()));
    }

    SqlNode fetch = null;
    Optional<String> limitStr = getTextIfPresent(context.limit);
    if (context.LIMIT() != null && limitStr.isPresent()) {
      fetch = SqlLiteral.createExactNumeric(limitStr.get(), getLocation(context.limit));
    }

    if (term instanceof SqlSelect) {
      // When we have a simple query specification
      // followed by order by limit, fold the order by and limit
      // clauses into the query specification (analyzer/planner
      // expects this structure to resolve references with respect
      // to columns defined in the query specification)
      SqlSelect query = (SqlSelect) term;
      query.setOrderBy(orderBy);
      query.setFetch(fetch);
      return query;
    }

    if (orderBy != null || fetch != null) {
      return new SqlOrderBy(
          getLocation(context),
          term,
          orderBy,
          null,
          fetch
      );
    }
    return term;
  }

  @Override
  public SqlNode visitQuerySpecification(QuerySpecificationContext context) {
    SqlNode from;
    List<SqlNode> selectItems = visit(context.selectItem(), SqlNode.class);

    List<SqlNode> relations = visit(context.relation(), SqlNode.class);
    if (relations.isEmpty()) {
      throw new RuntimeException("FROM required");
    } else {
      // cross joins
      Iterator<SqlNode> iterator = relations.iterator();
      SqlNode relation = iterator.next();

      while (iterator.hasNext()) {
        relation = new SqlJoin(getLocation(context),
            relation,
            SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
            SqlLiteral.createSymbol(JoinType.DEFAULT, SqlParserPos.ZERO),
            iterator.next(),
            SqlLiteral.createSymbol(JoinConditionType.NONE, SqlParserPos.ZERO),
            null
        );
      }

      from = relation;
    }

    SqlNodeList keywords = isDistinct(context.setQuantifier()) ?
        new SqlNodeList(List.of(
            SqlSelectKeyword.DISTINCT.symbol(getLocation(context.setQuantifier()))),
            getLocation(context.setQuantifier()))
        : null;
    SqlNodeList hints = getHints(context.hint());
    return new SqlSelect(
        getLocation(context),
        keywords,
        new SqlNodeList(selectItems, getLocation(context.SELECT())),
        from,
        visitIfPresent(context.where, SqlNode.class).orElse(null),
        visitIfPresent(context.groupBy(), SqlNodeList.class).orElse(null),
        visitIfPresent(context.having, SqlNode.class).orElse(null),
        null, null, null, null,
        hints);
  }

  @Override
  public SqlNode visitGroupBy(GroupByContext context) {
    return visit(context.groupingElement());
  }

  @Override
  public SqlNode visitSingleGroupingSet(SingleGroupingSetContext context) {
    List<SqlNode> groups = visit(context.groupingSet().expression(), SqlNode.class);
    return new SqlNodeList(groups, getLocation(context));
  }

  @Override
  public SqlNode visitSetOperation(SetOperationContext context) {
    SqlNode left = visit(context.left);
    SqlNode right = visit(context.right);

    Optional<Boolean> distinct = Optional.empty();
    if (context.setQuantifier() != null) {
      if (context.setQuantifier().DISTINCT() != null) {
        distinct = Optional.of(true);
      } else if (context.setQuantifier().ALL() != null) {
        distinct = Optional.of(false);
      }
    }

    switch (context.operator.getType()) {
      case SqlBaseLexer.UNION:
        SqlSetOperator op = distinct.map(d -> SqlStdOperatorTable.UNION_ALL)
            .orElse(SqlStdOperatorTable.UNION);
        return op.createCall(getLocation(context.UNION()), List.of(left, right));
      case SqlBaseLexer.INTERSECT:
        SqlSetOperator op_i = distinct.map(d -> SqlStdOperatorTable.UNION_ALL)
            .orElse(SqlStdOperatorTable.UNION);
        return op_i.createCall(getLocation(context.INTERSECT()), List.of(left, right));
      case SqlBaseLexer.EXCEPT:
        SqlSetOperator op_e = distinct.map(d -> SqlStdOperatorTable.UNION_ALL)
            .orElse(SqlStdOperatorTable.UNION);
        return op_e.createCall(getLocation(context.EXCEPT()), List.of(left, right));
    }

    throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
  }

  @Override
  public SqlNode visitSelectAll(SelectAllContext context) {
    return SqlIdentifier.star(getLocation(context.ASTERISK()));
  }

  @Override
  public SqlNode visitSelectSingle(SelectSingleContext context) {
    SqlNode expression = visit(context.expression());

//    if (expression instanceof Identifier &&
//        ((Identifier) expression).getNamePath().getLast().getCanonical()
//        .equalsIgnoreCase("*")) {
//      return new AllColumns(getLocation(context),
//          ((Identifier) expression).getNamePath().getPrefix().get());
//    }

    Optional<SqlNode> alias = visitIfPresent(context.identifier(), SqlNode.class);
    return alias.map(a -> (SqlNode) SqlStdOperatorTable.AS.createCall(getLocation(context),
            expression, a))
        .orElse(expression);
  }

  @Override
  public SqlNode visitSubquery(SubqueryContext context) {
    return visit(context.queryNoWith());
  }

  // ********************* primary expressions **********************

  @Override
  public SqlNode visitParameter(ParameterContext ctx) {
    return new SqlNamedDynamicParam(
        (SqlIdentifier) visit(ctx.identifier()),
        getLocation(ctx)
    );
  }

  @Override
  public SqlNode visitLogicalNot(LogicalNotContext context) {
    return SqlStdOperatorTable.NOT.createCall(getLocation(context),
        visit(context.booleanExpression()));
  }

  @Override
  public SqlNode visitLogicalBinary(LogicalBinaryContext context) {
    SqlOperator operator = getLogicalBinaryOperator(context.operator);
    SqlNode left = visit(context.left);
    SqlNode right = visit(context.right);

    return operator.createCall(
        getLocation(context.operator),
        left,
        right);
    //    boolean warningForMixedAndOr = false;
//
//    if (operator.equals(LogicalBinaryExpression.Operator.OR) &&
//        (mixedAndOrOperatorParenthesisCheck(right, context.right,
//            LogicalBinaryExpression.Operator.AND) ||
//            mixedAndOrOperatorParenthesisCheck(left, context.left,
//                LogicalBinaryExpression.Operator.AND))) {
//      warningForMixedAndOr = true;
//    }
//
//    if (operator.equals(LogicalBinaryExpression.Operator.AND) &&
//        (mixedAndOrOperatorParenthesisCheck(right, context.right,
//            LogicalBinaryExpression.Operator.OR) ||
//            mixedAndOrOperatorParenthesisCheck(left, context.left,
//                LogicalBinaryExpression.Operator.OR))) {
//      warningForMixedAndOr = true;
//    }
//
//    if (warningForMixedAndOr) {
//      warningConsumer.accept(new ParsingWarning(
//          "The Query contains OR and AND operator without proper parenthesis. "
//              + "Make sure the operators are guarded by parenthesis in order "
//              + "to fetch logically correct results",
//          context.getStart().getLine(), context.getStart().getCharPositionInLine()));
//    }
  }

  @Override
  public SqlNode visitRelationDefault(RelationDefaultContext ctx) {
    return visit(ctx.aliasedRelation());
  }

  @Override
  public SqlNode visitSubqueryRelation(SubqueryRelationContext ctx) {
    return visit(ctx.query());
  }

  @Override
  public SqlNode visitJoinSetOperation(JoinSetOperationContext ctx) {
    //todo
    return new SqrlJoinSetOperation(getLocation(ctx));
  }

  @Override
  public SqlNode visitJoinPath(JoinPathContext ctx) {
    List<SqlNode> relations = new ArrayList<>();
    List<SqlNode> conditions = new ArrayList<>();
    for (int i = 0; i < ctx.joinPathCondition().size(); i++) {
      SqlNode relation = ctx.joinPathCondition(i).aliasedRelation().accept(this);
      relations.add(relation);
      SqlNode condition = ctx.joinPathCondition(i).joinCondition() == null ? null :
          ctx.joinPathCondition(i).joinCondition().booleanExpression().accept(this);
      conditions.add(condition);
    }

    return new SqrlJoinPath(getLocation(ctx),
        relations,
        conditions);
  }

  @Override
  public SqlNode visitJoinRelation(JoinRelationContext context) {
    SqlNode left = visit(context.left);
    SqlNode right;

    //todo
    if (context.CROSS() != null) {
      right = visit(context.right);
      return new SqlJoin(getLocation(context),
          left,
          SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
          JoinType.CROSS.symbol(SqlParserPos.ZERO),
          right,
          JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
          null);
    }

    SqlLiteral type = JoinConditionType.NONE.symbol(SqlParserPos.ZERO);
    SqlNode condition = null;
    right = visit(context.rightRelation);
    if (context.joinCondition() != null && context.joinCondition().ON() != null) {
      type = JoinConditionType.ON.symbol(getLocation(context.joinCondition().ON()));
      condition = visit(context.joinCondition().booleanExpression());
    }

    return new SqlJoin(getLocation(context),
        left,
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        toJoinType(context.joinType()).symbol(getLocation(context.joinType())),
        right,
        type,
        condition);
  }

  public JoinType toJoinType(JoinTypeContext joinTypeContext) {
    JoinType joinType;

    if (joinTypeContext.INNER() != null) {
      joinType = JoinType.INNER;
    } else if (joinTypeContext.TEMPORAL() != null) {
      joinType = JoinType.TEMPORAL;
    } else if (joinTypeContext.INTERVAL() != null) {
      joinType = JoinType.INTERVAL;
    } else {
      joinType = JoinType.DEFAULT;
    }

    return joinType;
  }

  @Override
  public SqlNode visitTableName(TableNameContext context) {
    SqlIdentifier name = getNamePath(context.qualifiedName());

    return name;
  }

  public SqlNodeList getHints(HintContext hint) {
    if (hint == null) {
      return new SqlNodeList(SqlParserPos.ZERO);
    }
    return new SqlNodeList(
        hint.hintItem().stream()
            .map(h -> visitHintItem(h))
            .collect(toList()),
        getLocation(hint));
  }

  @Override
  public SqlNode visitHintItem(HintItemContext ctx) {
    return new SqlHint(getLocation(ctx),
        (SqlIdentifier) ctx.identifier().accept(this),
        new SqlNodeList(visit(ctx.keyValue(), SqlNode.class), getLocation(ctx)),
        HintOptionFormat.ID_LIST
    );
  }

  @Override
  public SqlNode visitKeyValue(KeyValueContext ctx) {
    return visit(ctx.identifier());
  }

  @Override
  public SqlNode visitExpression(ExpressionContext ctx) {
    return visit(ctx.booleanExpression());
  }

  @Override
  public SqlNode visitValueExpressionDefault(ValueExpressionDefaultContext ctx) {
    if (ctx.primaryExpression().size() > 1) {
      return SqlStdOperatorTable.COALESCE.createCall(
          getLocation(ctx),
          visit(ctx.primaryExpression().get(0)),
          visit(ctx.primaryExpression().get(1)));
    }
    return visit(ctx.primaryExpression().get(0));
  }

  @Override
  public SqlNode visitPredicated(PredicatedContext context) {
    if (context.predicate() != null) {
      return visit(context.predicate());
    }

    return visit(context.valueExpression);
  }

  @Override
  public SqlNode visitComparison(ComparisonContext context) {
    SqlOperator op = getComparisonOperator(
        ((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol());

    return op.createCall(getLocation(context.comparisonOperator()), visit(context.value),
        visit(context.right));
  }

  @Override
  public SqlNode visitBetween(BetweenContext context) {
    SqlCall between = SqlStdOperatorTable.BETWEEN.createCall(
        getLocation(context),
        visit(context.value),
        visit(context.lower),
        visit(context.upper)
    );

    if (context.NOT() != null) {
      between = SqlStdOperatorTable.NOT.createCall(getLocation(context), between);
    }

    return between;
  }

  @Override
  public SqlNode visitNullPredicate(NullPredicateContext context) {
    SqlNode child = visit(context.value);

    if (context.NOT() == null) {
      return SqlStdOperatorTable.IS_NULL.createCall(getLocation(context), child);
    }

    return SqlStdOperatorTable.IS_NOT_NULL.createCall(getLocation(context), child);
  }

  @Override
  public SqlNode visitInList(InListContext context) {
    SqlNode result = SqlStdOperatorTable.IN.createCall(
        getLocation(context),
        (SqlNode) visit(context.value),
        new SqlNodeList(visit(context.expression(), SqlNode.class),
            getLocation(context)));

    if (context.NOT() != null) {
      result = SqlStdOperatorTable.NOT.createCall(getLocation(context), result);
    }

    return result;
  }

  @Override
  public SqlNode visitInSubquery(InSubqueryContext context) {
    SqlNode query = visit(context.query());
    SqlNode result = SqlStdOperatorTable.IN.createCall(
        getLocation(context),
        visit(context.value),
        new SqlNodeList(List.of(query),
            getLocation(context)));

    if (context.NOT() != null) {
      result = SqlStdOperatorTable.NOT.createCall(getLocation(context), result);
    }

    return result;
  }

  @Override
  public SqlNode visitArithmeticUnary(ArithmeticUnaryContext context) {
    SqlNode child = visit(context.valueExpression());

    switch (context.operator.getType()) {
      case SqlBaseLexer.MINUS:
        return SqlStdOperatorTable.UNARY_MINUS.createCall(getLocation(context), child);
      case SqlBaseLexer.PLUS:
        return SqlStdOperatorTable.UNARY_PLUS.createCall(getLocation(context), child);
      default:
        throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
    }
  }

  @Override
  public SqlNode visitArithmeticBinary(ArithmeticBinaryContext context) {
    return getArithmeticBinaryOperator(context.operator)
        .createCall(getLocation(context.operator),
            List.of(visit(context.left),
                visit(context.right)));
  }

  @Override
  public SqlNode visitParenthesizedExpression(ParenthesizedExpressionContext context) {
    return visit(context.expression());
  }

  @Override
  public SqlNode visitCast(CastContext context) {
    SqlNode expr = visit(context.expression());
    SqlDataTypeSpec dataType = getType(context.type());
    return SqlStdOperatorTable.CAST.createCall(getLocation(context),
        List.of(expr, dataType));
  }

  @Override
  public SqlNode visitQualifiedName(QualifiedNameContext ctx) {
    SqlIdentifier qualified = getNamePath(ctx);
    return qualified;
  }

  @Override
  public SqlNode visitSubqueryExpression(SubqueryExpressionContext context) {
    return visit(context.query());
  }

  @Override
  public SqlNode visitColumnReference(ColumnReferenceContext context) {
    return visit(context.qualifiedName());
  }

  @Override
  public SqlNode visitSimpleCase(SimpleCaseContext context) {
//    visit(context.whenClause(), SqlNode.class);
    List<SqlNode> whenList = new ArrayList<>();
    List<SqlNode> thenList = new ArrayList<>();
    Optional<SqlNode> elseExpr = visitIfPresent(context.elseExpression, SqlNode.class);

    for (WhenClauseContext ctx : context.whenClause()) {
      whenList.add(visit(ctx.condition));
      thenList.add(visit(ctx.result));
    }

    return new SqlCase(getLocation(context),
        null,
        new SqlNodeList(whenList, getLocation(context)),
        new SqlNodeList(thenList, getLocation(context)),
        elseExpr.orElse(null));
  }

  @Override
  public SqlNode visitWhenClause(WhenClauseContext context) {
    throw new RuntimeException("see visitSimpleCase");
  }

  @Override
  public SqlNode visitLike(LikeContext ctx) {
    SqlNode value = ctx.valueExpression().accept(this);
    SqlNode pattern = ctx.pattern.accept(this);

    if (ctx.NOT() != null) {
      return SqlStdOperatorTable.NOT_LIKE
          .createCall(getLocation(ctx), value, pattern);
    }

    return SqlStdOperatorTable.LIKE.createCall(getLocation(ctx),
        value, pattern);
  }

  @Override
  public SqlNode visitExistsCall(ExistsCallContext context) {
    SqlNode query = context.query().accept(this);

    return SqlStdOperatorTable.EXISTS.createCall(getLocation(context), query);
  }

  @Override
  public SqlNode visitFunctionCall(FunctionCallContext context) {
//    boolean distinct = isDistinct(context.setQuantifier());
    SqlIdentifier name = (SqlIdentifier) context.qualifiedName().accept(this);
    List<SqlNode> args;
    if (context.expression() != null) {
      args = visit(context.expression(), SqlNode.class);
    } else if (context.query() != null){
      args = List.of(visit(context.query()));
    } else {
      throw new RuntimeException("Unknown function ast");
    }

    //special case: count(*)
    if (context.ASTERISK() != null) {
      args = List.of(SqlIdentifier.star(SqlParserPos.ZERO));
    }
    //special case: count()
    if (name.names.size() == 1 && name.names.get(0).equalsIgnoreCase("count")
        && args.isEmpty()) {
      args = List.of(SqlIdentifier.star(SqlParserPos.ZERO));
    }

    SqlUnresolvedFunction function = new SqlUnresolvedFunction(name,
        null, null, null, null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
    return function.createCall(getLocation(context), args);
  }

  // ***************** helpers *****************

  @Override
  public SqlNode visitSortItem(SortItemContext context) {
    SqlNode expression = visit(context.expression());

    SqlNode node = Optional.ofNullable(context.ordering)
        .flatMap(AstBuilder::getOrderingType)
        .map(o -> (SqlNode) o.createCall(getLocation(context), expression))
        .orElse(expression);

    return node;
  }

  @Override
  public SqlNode visitUnquotedIdentifier(UnquotedIdentifierContext context) {
    return new SqlIdentifier(List.of(context.getText()),
        null, getLocation(context), List.of(getLocation(context)));
  }

  @Override
  public SqlNode visitQuotedIdentifier(QuotedIdentifierContext context) {
    String token = context.getText();
    String identifier = token.substring(1, token.length() - 1)
        .replace("\"\"", "\"");

    return new SqlIdentifier(List.of(identifier),
        null, getLocation(context), List.of(getLocation(context)));
  }

  @Override
  public SqlNode visitNullLiteral(NullLiteralContext context) {
    return SqlLiteral.createNull(getLocation(context));
  }

  @Override
  public SqlNode visitBasicStringLiteral(BasicStringLiteralContext context) {
    return SqlLiteral.createCharString(unquote(context.STRING().getText()), getLocation(context));
  }

  @Override
  public SqlNode visitUnicodeStringLiteral(UnicodeStringLiteralContext context) {
    return SqlLiteral.createBinaryString(decodeUnicodeLiteral(context), getLocation(context));
  }

  @Override
  public SqlNode visitIntegerLiteral(IntegerLiteralContext context) {
    return SqlLiteral.createExactNumeric(context.getText(), getLocation(context));
  }

  @Override
  public SqlNode visitDecimalLiteral(DecimalLiteralContext context) {
    SqlLiteral literal = SqlLiteral.createExactNumeric(context.getText(), getLocation(context));

    return literal;
  }

  @Override
  public SqlNode visitDoubleLiteral(DoubleLiteralContext context) {
    return SqlLiteral.createExactNumeric(context.getText(), getLocation(context));
  }

  @Override
  public SqlNode visitIntervalLiteral(IntervalLiteralContext ctx) {
    return visit(ctx.interval());
  }

  @Override
  public SqlNode visitNumericLiteral(NumericLiteralContext ctx) {
    return visit(ctx.number());
  }

  @Override
  public SqlNode visitBooleanLiteral(BooleanLiteralContext ctx) {
    return visit(ctx.booleanValue());
  }

  @Override
  public SqlNode visitBooleanValue(BooleanValueContext context) {
    return SqlLiteral.createBoolean(Boolean.parseBoolean(context.getText()), getLocation(context));
  }

  @Override
  public SqlNode visitInterval(IntervalContext context) {
    int sign = Optional.ofNullable(context.sign)
        .map(AstBuilder::getIntervalSign)
        .orElse(1);
    SqlLiteral expr = (SqlLiteral) visit(context.number());
    TimeUnit timeUnit = getIntervalFieldType(
        (Token) context.intervalField().getChild(0).getPayload());
    //YEAR | MONTH | WEEK | DAY | HOUR | MINUTE | SECOND
    //Calcite adjusts dates to either months or milliseconds, otherwise it produces invalid semantics
    // However, there is no SqlTypeName.INTERVAL_MILLISECOND so we use seconds, even though
    // it gets converted to milliseconds when it gets converted to a relation

    switch (timeUnit) {
      case DECADE:
      case CENTURY:
      case MILLENNIUM:
      case YEAR:
      case MONTH:
//        BigDecimal newValue = expr.bigDecimalValue()
//            .multiply(timeUnit.multiplier);
//        expr = SqlLiteral.createExactNumeric(newValue.toString(), expr.getParserPosition());
//        timeUnit = TimeUnit.MONTH;
//        break;
      case DAY:
      case HOUR:
      case MINUTE:
      case SECOND:
      case QUARTER:
      case ISOYEAR:
      case WEEK:
      case MILLISECOND:
      case MICROSECOND:
      case NANOSECOND:
      case DOW:
      case ISODOW:
      case DOY:
      case EPOCH:
        //TODO: Normalize time for calcite so its less hard on the user
        // e.g 120 SECONDS => 2 MINUTES
//        BigDecimal newValue2 = expr.bigDecimalValue()
//            .multiply(timeUnit.multiplier)
//            .divide(BigDecimal.valueOf(1000));
//        expr = SqlLiteral.createExactNumeric(newValue2.toString(), expr.getParserPosition());
//        timeUnit = TimeUnit.SECOND;
        //normalized in sqltorel convert to seconds
    }

    return SqlLiteral.createInterval(sign,
        expr.toValue(),
        new SqlIntervalQualifier(timeUnit, null, getLocation(context.intervalField())),
        getLocation(context)
    );
  }

  private static int getIntervalSign(Token token) {
    if (token.getText().equalsIgnoreCase("PLUS")) {
      return 1;
    }
    if (token.getText().equalsIgnoreCase("MINUS")) {
      return -1;
    }
    throw new RuntimeException("unknown interval sign");
  }

  @Override
  public SqlNode visitScript(ScriptContext ctx) {
    return new ScriptNode(
        getLocation(ctx),
        visit(ctx.statement(), SqlNode.class)
    );
  }

  @Override
  public SqlNode visitImportStatement(ImportStatementContext ctx) {
    return visit(ctx.importDefinition());
  }

  @Override
  public SqlNode visitImportDefinition(ImportDefinitionContext ctx) {
    Optional<SqlIdentifier> alias = Optional.ofNullable(
        ctx.alias == null ? null : (SqlIdentifier) visit(ctx.alias));

    Optional<SqlNode> timestamp;
    Optional<SqlIdentifier> timestampAlias = Optional.empty();
    if (ctx.TIMESTAMP() != null) {
      SqlNode expr = visit(ctx.expression());

      if (ctx.timestampAlias != null) {
        SqlIdentifier tsAlias = ((SqlIdentifier) visit(ctx.timestampAlias));
        timestampAlias = Optional.of(tsAlias);
      }
      timestamp = Optional.of(expr);
    } else {
      timestamp = Optional.empty();
    }
    NamePath namePath = NamePath.of(getNamePath(ctx.qualifiedName()).names.stream()
        .map(i -> (i.equals(""))
            ? ReservedName.ALL
            : NameCanonicalizer.SYSTEM.name(i)
        )
        .collect(Collectors.toList())
    );
    return new ImportDefinition(getLocation(ctx), getNamePath(ctx.qualifiedName()), namePath, alias,
        timestamp, timestampAlias);
  }

  @Override
  public SqlNode visitExportStatement(ExportStatementContext ctx) {
    return visit(ctx.exportDefinition());
  }

  @Override
  public SqlNode visitExportDefinition(ExportDefinitionContext ctx) {
    return new ExportDefinition(getLocation(ctx),
        getNamePath(ctx.qualifiedName(0)), toNamePath(getNamePath(ctx.qualifiedName(0))),
        getNamePath(ctx.qualifiedName(1)));
  }

  @Override
  public SqlNode visitDistinctAssignment(DistinctAssignmentContext ctx) {
    SqlIdentifier namePath = getNamePath(ctx.qualifiedName());
    SqlIdentifier tableName = (SqlIdentifier) visit(ctx.table);

    Optional<SqlIdentifier> alias = Optional.empty();
    if (ctx.identifier().size() > 1) {
      alias = Optional.of((SqlIdentifier) visit(ctx.identifier(1)));
    }
    SqlNode aliasedName = alias.map(a -> (SqlNode) SqlStdOperatorTable.AS.createCall(
        getLocation(ctx.identifier(1)),
        tableName,
        a
    )).orElse(tableName);

    List<SqlNode> pk = new ArrayList<>();

    for (int i = 0; i < ctx.onList().expression().size(); i++) {
      SqlNode pkNode = visit(ctx.onList().expression(i));
      pk.add(pkNode);
    }

    List<SqlNode> sort = new ArrayList<>();
    if (ctx.orderExpr != null) {
      SqlNode order = visit(ctx.orderExpr);
      sort = List.of(SqlStdOperatorTable.DESC.createCall(getLocation(ctx.orderExpr), order));
    }

    return new DistinctAssignment(
        getLocation(ctx),
        namePath,
        toNamePath(namePath),
        aliasedName,
        pk,
        sort,
        emptyListToEmptyOptional(getHints(ctx.hint())),
        null
    );
  }

  private SqlIdentifier getNamePath(QualifiedNameContext context) {
    List<SqlIdentifier> ids = visit(context.identifier(), SqlIdentifier.class);
    SqlIdentifier id = flatten(ids);
    Preconditions.checkState(
        id.names.size() == SqrlUtil.getComponentPositions(id).size());

    if (context.all != null) {
      return id.plusStar();
    }

    return id;
  }

  private SqlIdentifier flatten(List<SqlIdentifier> ids) {
    List<String> names = new ArrayList<>();
    List<SqlParserPos> cPos = new ArrayList<>();
    for (SqlIdentifier i : ids) {
      names.addAll(i.names);
      cPos.addAll(SqrlUtil.getComponentPositions(i));
    }

    return new SqlIdentifier(names, ids.get(0).getCollation(), ids.get(0).getParserPosition(),
        cPos);
  }

  @Override
  public SqlNode visitJoinAssignment(JoinAssignmentContext ctx) {
    SqlIdentifier name = getNamePath(ctx.qualifiedName());

    SqlNode join = visit(ctx.joinSpecification());

    return new JoinAssignment(getLocation(ctx), name,
        toNamePath(name),
        getTableArgs(ctx.tableFunction()),
        join,
        emptyListToEmptyOptional(getHints(ctx.hint())));
  }

  private NamePath toNamePath(SqlIdentifier name) {
    return NamePath.of(name.names.stream()
        .map(e-> Name.system(e)) //todo: canonicalize
        .collect(toList()));
  }

  private Optional<List<TableFunctionArgument>> getTableArgs(TableFunctionContext ctx) {
    if (ctx == null) {
      return Optional.empty();
    }
    List<TableFunctionArgument> args = ctx.functionArgument().stream()
        .map(arg -> toFunctionArg(arg))
        .collect(toList());

    return Optional.of(args);
  }

  private TableFunctionArgument toFunctionArg(FunctionArgumentContext ctx) {
    return new TableFunctionArgument(
        (SqlIdentifier) visit(ctx.name),
        getType(ctx.typeName)
    );
  }

  @Override
  public SqlNode visitJoinSpecification(JoinSpecificationContext ctx) {
    List<SqlNode> sort = visit(ctx.sortItem(), SqlNode.class);
    Optional<SqlNodeList> sortList = emptyListToEmptyOptional(sort);

    Optional<SqlNumericLiteral> limit =
        ctx.limit == null || ctx.limit.getText().equalsIgnoreCase("ALL")
            ? Optional.empty()
            : Optional.of(
                SqlLiteral.createExactNumeric(ctx.limit.getText(), getLocation(ctx.limit)));
    SqrlJoinTerm relation = (SqrlJoinTerm) visit(ctx.joinTerm());

    Optional<SqlIdentifier> inverse = Optional.ofNullable(ctx.inv)
        .map(i -> (SqlIdentifier) visit(i));
    return new SqrlJoinDeclarationSpec(
        getLocation(ctx),
        relation,
        sortList,
        limit,
        inverse,
        Optional.empty()
    );
  }

  @Override
  public SqlNode visitQueryAssign(QueryAssignContext ctx) {
    SqlNode query = visit(ctx.query());
    return new QueryAssignment(getLocation(ctx), getNamePath(ctx.qualifiedName()),
        toNamePath(getNamePath(ctx.qualifiedName())),
        getTableArgs(ctx.tableFunction()),
        query,
        emptyListToEmptyOptional(getHints(ctx.hint())));
  }

  @Override
  public SqlNode visitStreamAssign(StreamAssignContext ctx) {

    SqlNode query = visit(ctx.streamQuery().query());
    SubscriptionType type = SubscriptionType.valueOf(
        ctx.streamQuery().subscriptionType().getText());
    return new StreamAssignment(getLocation(ctx), getNamePath(ctx.qualifiedName()),
        toNamePath(getNamePath(ctx.qualifiedName())),
        getTableArgs(ctx.tableFunction()),
        query,
        type,
        emptyListToEmptyOptional(getHints(ctx.hint())));
  }

  private Optional<SqlNodeList> emptyListToEmptyOptional(List<SqlNode> list) {
    if (list == null || list.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new SqlNodeList(list, SqlParserPos.ZERO));
  }

  private Optional<SqlNodeList> emptyListToEmptyOptional(SqlNodeList list) {
    if (list.getList().isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(list);
  }

  @Override
  public SqlNode visitExpressionAssign(ExpressionAssignContext ctx) {
    SqlIdentifier name = getNamePath(ctx.qualifiedName());
    SqlNode expr = visit(ctx.expression());
    return new ExpressionAssignment(getLocation(ctx), name,
        toNamePath(name),
        getTableArgs(ctx.tableFunction()),
        expr,
        emptyListToEmptyOptional(getHints(ctx.hint())));
  }

  @Override
  public SqlNode visitBackQuotedIdentifier(BackQuotedIdentifierContext ctx) {
    return new SqlIdentifier(List.of(ctx.getText().substring(1, ctx.getText().length() - 1)), null,
        getLocation(ctx), List.of(getLocation(ctx)));
  }

  @Override
  protected SqlNode defaultResult() {
    return null;
  }

  @Override
  protected SqlNode aggregateResult(SqlNode aggregate, SqlNode nextResult) {
    return null;
  }

  private <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
    return Optional.ofNullable(context)
        .map(this::visit)
        .map(clazz::cast);
  }

  private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
    return contexts.stream()
        .map(this::visit)
        .map(clazz::cast)
        .collect(toList());
  }

  @Override
  public SqlNode visitStringLiteral(StringLiteralContext ctx) {
    return visit(ctx.string());
  }

  private SqlDataTypeSpec getType(TypeContext ctx) {
    return new SqlDataTypeSpec(getTypeName(ctx.baseType()), getLocation(ctx));
  }

  private SqlTypeNameSpec getTypeName(BaseTypeContext baseType) {
    //todo: Collections, params, dates, etc
    SqlIdentifier id = (SqlIdentifier) visit(baseType.identifier());

    String name = Util.last(id.names);
    SqlTypeName typeName;
    switch (name.toLowerCase(Locale.ROOT)) {
      case "boolean":
        typeName = SqlTypeName.BOOLEAN;
        break;
      case "datetime":
        return new SqlBasicTypeNameSpec(
            SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
            3,
            getLocation(baseType)
        );
      case "float":
        typeName = SqlTypeName.FLOAT;
        break;
      case "integer":
        typeName = SqlTypeName.INTEGER;
        break;
      case "string":
        typeName = SqlTypeName.VARCHAR;
        break;
      default:
        throw new RuntimeException(String.format("Unknown data type %s", name));
    }

    return new SqlBasicTypeNameSpec(
        typeName,
        getLocation(baseType)
    );
  }

  @Override
  public SqlNode visitQueryTermDefault(QueryTermDefaultContext ctx) {
    return visit(ctx.queryPrimary());
  }

  @Override
  public SqlNode visitQueryPrimaryDefault(QueryPrimaryDefaultContext ctx) {
    return visit(ctx.querySpecification());
  }

  @Override
  public SqlNode visitAliasedRelation(AliasedRelationContext ctx) {
    if (ctx.identifier() == null) {
      return visit(ctx.relationPrimary());
    }
    return SqlStdOperatorTable.AS.createCall(
        getLocation(ctx),
        visit(ctx.relationPrimary()),
        visit(ctx.identifier()));
  }

//  @Override
//  public SqlNode visitChildren(RuleNode node) {
//    throw new RuntimeException(node.getClass().getName());
//    return super.visitChildren(node);
//  }

  private enum UnicodeDecodeState {
    EMPTY,
    ESCAPED,
    UNICODE_SEQUENCE
  }
}
