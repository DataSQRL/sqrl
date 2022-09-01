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
package ai.datasqrl.parse;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import ai.datasqrl.parse.SqlBaseParser.*;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.hints.SqrlHintStrategyTable;
import ai.datasqrl.schema.TableFunctionArgument;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.util.Strings;

/**
 * Builds the abstract syntax tree for an SQRL script using the classes in
 * {@link ai.datasqrl.parse.tree}.
 */
class AstBuilder
    extends SqlBaseBaseVisitor<SqlNode> {

  AstBuilder(ParsingOptions parsingOptions) {
  }
//
//  @SneakyThrows
//  private SqlNode parseSql(String sql) {
//    SqlNode node = SqlParser.create(sql,
//        SqlParser.config().withCaseSensitive(false)
//            .withConformance(SqrlConformance.INSTANCE)
//            .withUnquotedCasing(Casing.UNCHANGED)
//        ).parseQuery();
//    return node;
//  }

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
      case SqlBaseLexer.IS:
        throw new RuntimeException();
//        return SqlStdOperatorTable.IS_NOT_NULL;
    }

    throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
  }

  private static TimeUnit getIntervalFieldType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.YEAR:
        return TimeUnit.YEAR;
      case SqlBaseLexer.MONTH:
        return TimeUnit.MONTH;
      case SqlBaseLexer.DAY:
        return TimeUnit.DAY;
      case SqlBaseLexer.WEEK:
        return TimeUnit.WEEK;
      case SqlBaseLexer.HOUR:
        return TimeUnit.HOUR;
      case SqlBaseLexer.MINUTE:
        return TimeUnit.MINUTE;
      case SqlBaseLexer.SECOND:
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
      return query;
    }

    return new SqlOrderBy(
        getLocation(context),
        term,
        orderBy,
        null,
        fetch
    );
  }

  @Override
  public SqlNode visitQuerySpecification(QuerySpecificationContext context) {
    SqlCall from;
    List<SqlNode> selectItems = visit(context.selectItem(), SqlNode.class);

    List<SqlCall> relations = visit(context.relation(), SqlCall.class);
    if (relations.isEmpty()) {
      throw new RuntimeException("FROM required");
    } else {
      // synthesize implicit join nodes
      Iterator<SqlCall> iterator = relations.iterator();
      SqlCall relation = iterator.next();

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
  public SqlNode visitJoinRelation(JoinRelationContext context) {
    SqlNode left = visit(context.left);
    SqlNode right;

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
    SqlNode criteria = null;
    right = (SqlNode) visit(context.rightRelation);
    if (context.joinCriteria() != null && context.joinCriteria().ON() != null) {
      type = JoinConditionType.ON.symbol(getLocation(context.joinCriteria().ON()));
      criteria = visit(context.joinCriteria().booleanExpression());
    }

    return new SqlJoin(getLocation(context),
        left,
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        toJoinType(context.joinType()).symbol(getLocation(context.joinType())),
        right,
        type,
        criteria);
  }

  public JoinType toJoinType(JoinTypeContext joinTypeContext) {
    JoinType joinType;

    if (joinTypeContext.INNER() != null) {
      joinType = JoinType.INNER;
    } else if (joinTypeContext.TEMPORAL() != null) {
      joinType = JoinType.TEMPORAL;
    } else {
      joinType = JoinType.DEFAULT;
    }

    return joinType;
  }

  @Override
  public SqlNode visitTableName(TableNameContext context) {
    Optional<SqlNode> alias = Optional.empty();
    if (context.identifier() != null) {
      alias = Optional.of(visit(context.identifier()));
    }

    Pair<List<String>, List<SqlParserPos>> name = getQualified(context.qualifiedName());
    SqlIdentifier tableName = new SqlIdentifier(name.getLeft(), null,
        getLocation(context), name.getRight());

    SqlNodeList hints = getHints(context.hint());

    SqlNode from = tableName;
    if (hints != null) {
      from = new SqlTableRef(getLocation(context), tableName, hints);
    }

    final SqlNode lastFrom = from;

    return alias.map(
            a -> (SqlNode) SqlStdOperatorTable.AS.createCall(getLocation(context), lastFrom, a))
        .orElse(from);
  }

  private Pair<List<String>, List<SqlParserPos>> getQualified(QualifiedNameContext context) {
    List<SqlIdentifier> parts = visit(context.identifier(), SqlIdentifier.class);
    List<String> names = new ArrayList<>();
    List<SqlParserPos> pos = new ArrayList<>();

    for (SqlIdentifier part : parts) {
      names.add(part.names.get(0));
      pos.add(part.getParserPosition());
    }
    if (context.all != null) {
      names.add("");
      pos.add(getLocation(context.all));
    }

    return Pair.of(names, pos);
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
        SqlNodeList.EMPTY,
        HintOptionFormat.EMPTY
    );
  }

  @Override
  public SqlNode visitExpression(ExpressionContext ctx) {
    return visit(ctx.booleanExpression());
  }

  @Override
  public SqlNode visitValueExpressionDefault(ValueExpressionDefaultContext ctx) {
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
    var qualified = getQualified(ctx);
    return new SqlIdentifier(qualified.getKey(), null, getLocation(ctx), qualified.getRight());
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
    visit(context.whenClause(), SqlNode.class);
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
  public SqlNode visitFunctionCall(FunctionCallContext context) {
//    boolean distinct = isDistinct(context.setQuantifier());
    SqlIdentifier name = (SqlIdentifier) context.qualifiedName().accept(this);
    List<SqlNode> args = visit(context.expression(), SqlNode.class);

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
    return new SqlIdentifier(List.of(context.getText()), getLocation(context));
  }

  @Override
  public SqlNode visitQuotedIdentifier(QuotedIdentifierContext context) {
    String token = context.getText();
    String identifier = token.substring(1, token.length() - 1)
        .replace("\"\"", "\"");

    return new SqlIdentifier(List.of(identifier), getLocation(context));
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
    return SqlLiteral.createExactNumeric(context.getText(), getLocation(context));
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
    SqlNode expr = visit(context.expression());
    TimeUnit timeUnit = getIntervalFieldType(
        (Token) context.intervalField().getChild(0).getPayload());
    return SqlLiteral.createInterval(sign,
        expr.toString(),
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
        visit(ctx.statement(), SqlNode.class));
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
    if (ctx.TIMESTAMP() != null) {

//      Interval tableI = new Interval(
//          ctx.expression().start.getStartIndex(),
//          ctx.expression().stop.getStopIndex());
//      String expr = ctx.expression().start.getInputStream().getText(tableI);

      SqlNode expr = visit(ctx.expression());
      if (ctx.timestampAlias != null) {
        SqlIdentifier timestampAlias = ((SqlIdentifier) visit(ctx.timestampAlias));
        SqlCall call = SqlStdOperatorTable.AS.createCall(getLocation(ctx), expr, timestampAlias);
        timestamp = Optional.of(call);
      } else {
        timestamp = Optional.of(expr);
      }
    } else {
      timestamp = Optional.empty();
    }

    return new ImportDefinition(getLocation(ctx), getNamePath(ctx.qualifiedName()), alias.map(
        a -> Name.system(String.join(".", a.names))
    ),
        timestamp);
  }

  @Override
  public SqlNode visitExportStatement(ExportStatementContext ctx) {
    return visit(ctx.exportDefinition());
  }

  @Override
  public SqlNode visitExportDefinition(ExportDefinitionContext ctx) {
    return new ExportDefinition(getLocation(ctx),
        getNamePath(ctx.qualifiedName(0)), getNamePath(ctx.qualifiedName(1)));
  }

  @Override
  public SqlNode visitAssign(AssignContext context) {
    return context.assignment().accept(this);
  }

  @Override
  public SqlNode visitDistinctAssignment(DistinctAssignmentContext ctx) {
    NamePath namePath = getNamePath(ctx.qualifiedName());
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
    //starts at 1
    for (int i = 1; i < ctx.identifier().size(); i++) {
      pk.add(visit(ctx.identifier(i)));
    }

    SqlNode order = visit(ctx.orderExpr);
    SqlCall sort = SqlStdOperatorTable.DESC.createCall(getLocation(ctx.orderExpr), order);

    SqlParserPos loc = getLocation(ctx);
    SqlNode query =
        new SqlSelect(
            loc,
            null,
            new SqlNodeList(List.of(SqlIdentifier.star(loc)), loc),
            aliasedName,
            null,
            null,
            null,
            null,
            new SqlNodeList(pk, getLocation(ctx)),
            null,
            SqlLiteral.createExactNumeric("1", getLocation(ctx)),
            new SqlNodeList(List.of(new SqlHint(loc,
                new SqlIdentifier(SqrlHintStrategyTable.TOP_N, loc),
                SqlNodeList.EMPTY,
                HintOptionFormat.EMPTY
            )), loc)
        );

    return new DistinctAssignment(
        getLocation(ctx),
        namePath,
        aliasedName,
        pk,
        List.of(sort),
        getHints(ctx.hint()),
        query
    );
  }

  private NamePath getNamePath(QualifiedNameContext context) {
    List<Name> parts = visit(context.identifier(), SqlIdentifier.class).stream()
        .map(s -> toNamePath(s)) // TODO: preserve quotedness
        .flatMap(e -> e.stream())
        .collect(Collectors.toList());
    if (context.all != null) {
      parts = new ArrayList<>(parts);
      parts.add(ReservedName.ALL);
    }

    return NamePath.of(parts);
  }

  private List<Name> toNamePath(SqlIdentifier s) {
    return s.names.stream()
        .map(e -> Name.system(e))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public SqlNode visitJoinAssignment(JoinAssignmentContext ctx) {
    NamePath name = getNamePath(ctx.qualifiedName());

    SqlNode join = visit(ctx.inlineJoin());

    return new JoinAssignment(getLocation(ctx), name,
        getTableArgs(ctx.tableFunction()),
        join,
        getHints(ctx.hint()));
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
        (SqlIdentifier)visit(ctx.name),
        getType(ctx.typeName)
    );
  }

  @Override
  public SqlNode visitInlineJoin(InlineJoinContext ctx) {
    List<SqlNode> sort = visit(ctx.sortItem(), SqlNode.class);

    SqlNode limit = ctx.limit == null || ctx.limit.getText().equalsIgnoreCase("ALL") ? null :
        Optional.of(SqlLiteral.createExactNumeric(ctx.limit.getText(), getLocation(ctx.limit)))
            .orElse(null);

    SqlNode from = visit(ctx.inlineJoinSpec());

    if (sort != null && sort.size() > 0 || limit != null) {
      return new SqlOrderBy(getLocation(ctx), from,
          sort != null ? new SqlNodeList(sort, getLocation(ctx)) : null,
          null, limit);
    }

    return from;
  }


  @Override
  public SqlNode visitInlineQueryTermDefault(InlineQueryTermDefaultContext ctx) {
    List<SqlNode> relations = visit(ctx.inlineAliasedJoinRelation(), SqlNode.class);
    if (relations.size() == 1) {
      SqlNode criteria = null;
      if (ctx.joinCriteria(0) != null && ctx.joinCriteria(0).ON() != null) {
        criteria = visit(ctx.joinCriteria(0).booleanExpression());
      }

      return new SqlSelect(getLocation(ctx),
          null,
          new SqlNodeList(List.of(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)), SqlParserPos.ZERO),
          relations.get(0), criteria,
          null, null,
          null, null, null,
          null,null);
    }


    SqlNode left = relations.get(0);
    for (int i = 1; i < relations.size(); i++) {
      JoinType joinType = toJoinType(ctx.joinType(i));

      SqlLiteral type = JoinConditionType.NONE.symbol(getLocation(ctx));
      SqlNode criteria = null;
      if (ctx.joinCriteria(i) != null && ctx.joinCriteria(i).ON() != null) {
        type = JoinConditionType.ON.symbol(getLocation(ctx.joinCriteria(i).ON()));
        criteria = visit(ctx.joinCriteria(i).booleanExpression());
      }
      left = new SqlJoin(getLocation(ctx),
          left,
          SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
          SqlLiteral.createSymbol(joinType, SqlParserPos.ZERO),
          relations.get(i),
          type,
          criteria);
    }

    return left;
  }

  @Override
  public SqlNode visitInlineSetOperation(InlineSetOperationContext context) {
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
    }

    throw new RuntimeException("unknown set operation");
  }

  @Override
  public SqlNode visitInlineTableName(InlineTableNameContext ctx) {
    SqlIdentifier identifier = (SqlIdentifier) visit(ctx.qualifiedName());
    SqlNodeList hints = getHints(ctx.hint());
    SqlNode from = new SqlTableRef(getLocation(ctx), identifier, hints);
    if (ctx.identifier() != null) {
      SqlIdentifier alias = (SqlIdentifier) visit(ctx.identifier());
      return SqlStdOperatorTable.AS.createCall(getLocation(ctx), from, alias);
    }

    return from;
  }

  @Override
  public SqlNode visitQueryAssign(QueryAssignContext ctx) {
    SqlNode query = visit(ctx.query());
    return new QueryAssignment(getLocation(ctx), getNamePath(ctx.qualifiedName()),
        getTableArgs(ctx.tableFunction()),
        query,
        getHints(ctx.hint()));
  }

  @Override
  public SqlNode visitExpressionAssign(ExpressionAssignContext ctx) {
    NamePath name = getNamePath(ctx.qualifiedName());
    SqlNode expr = visit(ctx.expression());
    return new ExpressionAssignment(getLocation(ctx), name,
        getTableArgs(ctx.tableFunction()),
        expr,
        getHints(ctx.hint()));
  }

  @Override
  public SqlNode visitCreateSubscription(CreateSubscriptionContext ctx) {

    return new CreateSubscription(
        getLocation(ctx),
        SubscriptionType.valueOf(ctx.subscriptionType().getText()),
        getNamePath(ctx.qualifiedName()),
        visit(ctx.query())
    );
  }

  @Override
  public SqlNode visitBackQuotedIdentifier(BackQuotedIdentifierContext ctx) {
    return new SqlIdentifier(ctx.getText(), getLocation(ctx));
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
//
//  private NamePath getNamePath(QualifiedNameContext context) {
//    List<Name> parts = visit(context.identifier(), Identifier.class).stream()
//        .map(Identifier::getNamePath) // TODO: preserve quotedness
//        .flatMap(e -> e.stream())
//        .collect(Collectors.toList());
//    if (context.all != null) {
//      parts = new ArrayList<>(parts);
//      parts.add(ReservedName.ALL);
//    }
//
//    return NamePath.of(parts);
//  }

  @Override
  public SqlNode visitStringLiteral(StringLiteralContext ctx) {
    return SqlLiteral.createCharString(ctx.getText(), getLocation(ctx));
  }

  private SqlDataTypeSpec getType(TypeContext ctx) {
    return new SqlDataTypeSpec(getTypeName(ctx.baseType()), getLocation(ctx));
  }

  private SqlTypeNameSpec getTypeName(BaseTypeContext baseType) {
    //todo: Collections, params, etc
    return new SqlUserDefinedTypeNameSpec(
        (SqlIdentifier) visit(baseType.identifier()),
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
