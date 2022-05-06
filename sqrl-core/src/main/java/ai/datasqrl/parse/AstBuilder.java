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

import ai.datasqrl.parse.SqlBaseParser.ArithmeticBinaryContext;
import ai.datasqrl.parse.SqlBaseParser.ArithmeticUnaryContext;
import ai.datasqrl.parse.SqlBaseParser.AssignContext;
import ai.datasqrl.parse.SqlBaseParser.BackQuotedIdentifierContext;
import ai.datasqrl.parse.SqlBaseParser.BasicStringLiteralContext;
import ai.datasqrl.parse.SqlBaseParser.BetweenContext;
import ai.datasqrl.parse.SqlBaseParser.BooleanExpressionContext;
import ai.datasqrl.parse.SqlBaseParser.BooleanValueContext;
import ai.datasqrl.parse.SqlBaseParser.CastContext;
import ai.datasqrl.parse.SqlBaseParser.ColumnReferenceContext;
import ai.datasqrl.parse.SqlBaseParser.ComparisonContext;
import ai.datasqrl.parse.SqlBaseParser.CreateSubscriptionContext;
import ai.datasqrl.parse.SqlBaseParser.DecimalLiteralContext;
import ai.datasqrl.parse.SqlBaseParser.DistinctAssignmentContext;
import ai.datasqrl.parse.SqlBaseParser.DoubleLiteralContext;
import ai.datasqrl.parse.SqlBaseParser.ExpressionAssignContext;
import ai.datasqrl.parse.SqlBaseParser.ExpressionContext;
import ai.datasqrl.parse.SqlBaseParser.FunctionCallContext;
import ai.datasqrl.parse.SqlBaseParser.GroupByContext;
import ai.datasqrl.parse.SqlBaseParser.ImportDefinitionContext;
import ai.datasqrl.parse.SqlBaseParser.ImportStatementContext;
import ai.datasqrl.parse.SqlBaseParser.InListContext;
import ai.datasqrl.parse.SqlBaseParser.InRelationContext;
import ai.datasqrl.parse.SqlBaseParser.InSubqueryContext;
import ai.datasqrl.parse.SqlBaseParser.InlineJoinBodyContext;
import ai.datasqrl.parse.SqlBaseParser.InlineJoinContext;
import ai.datasqrl.parse.SqlBaseParser.IntegerLiteralContext;
import ai.datasqrl.parse.SqlBaseParser.IntervalContext;
import ai.datasqrl.parse.SqlBaseParser.JoinAssignmentContext;
import ai.datasqrl.parse.SqlBaseParser.JoinRelationContext;
import ai.datasqrl.parse.SqlBaseParser.JoinTypeContext;
import ai.datasqrl.parse.SqlBaseParser.LogicalBinaryContext;
import ai.datasqrl.parse.SqlBaseParser.LogicalNotContext;
import ai.datasqrl.parse.SqlBaseParser.NullLiteralContext;
import ai.datasqrl.parse.SqlBaseParser.NullPredicateContext;
import ai.datasqrl.parse.SqlBaseParser.ParenthesizedExpressionContext;
import ai.datasqrl.parse.SqlBaseParser.PredicatedContext;
import ai.datasqrl.parse.SqlBaseParser.QualifiedNameContext;
import ai.datasqrl.parse.SqlBaseParser.QueryAssignContext;
import ai.datasqrl.parse.SqlBaseParser.QueryContext;
import ai.datasqrl.parse.SqlBaseParser.QueryNoWithContext;
import ai.datasqrl.parse.SqlBaseParser.QuerySpecificationContext;
import ai.datasqrl.parse.SqlBaseParser.QuotedIdentifierContext;
import ai.datasqrl.parse.SqlBaseParser.ScriptContext;
import ai.datasqrl.parse.SqlBaseParser.SelectAllContext;
import ai.datasqrl.parse.SqlBaseParser.SelectSingleContext;
import ai.datasqrl.parse.SqlBaseParser.SetOperationContext;
import ai.datasqrl.parse.SqlBaseParser.SetQuantifierContext;
import ai.datasqrl.parse.SqlBaseParser.SimpleCaseContext;
import ai.datasqrl.parse.SqlBaseParser.SingleGroupingSetContext;
import ai.datasqrl.parse.SqlBaseParser.SingleStatementContext;
import ai.datasqrl.parse.SqlBaseParser.SortItemContext;
import ai.datasqrl.parse.SqlBaseParser.SubqueryContext;
import ai.datasqrl.parse.SqlBaseParser.SubqueryExpressionContext;
import ai.datasqrl.parse.SqlBaseParser.TableNameContext;
import ai.datasqrl.parse.SqlBaseParser.TypeContext;
import ai.datasqrl.parse.SqlBaseParser.TypeParameterContext;
import ai.datasqrl.parse.SqlBaseParser.UnicodeStringLiteralContext;
import ai.datasqrl.parse.SqlBaseParser.UnquotedIdentifierContext;
import ai.datasqrl.parse.SqlBaseParser.ValueExpressionDefaultContext;
import ai.datasqrl.parse.SqlBaseParser.WhenClauseContext;
import ai.datasqrl.parse.tree.AllColumns;
import ai.datasqrl.parse.tree.ArithmeticBinaryExpression;
import ai.datasqrl.parse.tree.ArithmeticUnaryExpression;
import ai.datasqrl.parse.tree.BetweenPredicate;
import ai.datasqrl.parse.tree.BooleanLiteral;
import ai.datasqrl.parse.tree.Cast;
import ai.datasqrl.parse.tree.ComparisonExpression;
import ai.datasqrl.parse.tree.ComparisonExpression.Operator;
import ai.datasqrl.parse.tree.CreateSubscription;
import ai.datasqrl.parse.tree.DecimalLiteral;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.DoubleLiteral;
import ai.datasqrl.parse.tree.Except;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.GroupBy;
import ai.datasqrl.parse.tree.GroupingElement;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.InListExpression;
import ai.datasqrl.parse.tree.InPredicate;
import ai.datasqrl.parse.tree.JoinDeclaration;
import ai.datasqrl.parse.tree.Intersect;
import ai.datasqrl.parse.tree.IntervalLiteral;
import ai.datasqrl.parse.tree.IsNotNullPredicate;
import ai.datasqrl.parse.tree.IsNullPredicate;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.JoinCriteria;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.LogicalBinaryExpression;
import ai.datasqrl.parse.tree.LongLiteral;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.NotExpression;
import ai.datasqrl.parse.tree.NullLiteral;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.QueryBody;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SimpleCaseExpression;
import ai.datasqrl.parse.tree.SimpleGroupBy;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.parse.tree.StringLiteral;
import ai.datasqrl.parse.tree.SubqueryExpression;
import ai.datasqrl.parse.tree.SubscriptionType;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.TableSubquery;
import ai.datasqrl.parse.tree.Union;
import ai.datasqrl.parse.tree.WhenClause;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import ai.datasqrl.parse.tree.name.NamePath;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;

class AstBuilder
    extends SqlBaseBaseVisitor<Node> {

  private final ParsingOptions parsingOptions;
  private final Consumer<ParsingWarning> warningConsumer;
  private int parameterPosition;

  AstBuilder(ParsingOptions parsingOptions) {
    this.parsingOptions = requireNonNull(parsingOptions, "parsingOptions is null");
    this.warningConsumer = requireNonNull(parsingOptions.getWarningConsumer(),
        "warningConsumer is null");
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

  private static ArithmeticBinaryExpression.Operator getArithmeticBinaryOperator(Token operator) {
    switch (operator.getType()) {
      case SqlBaseLexer.PLUS:
        return ArithmeticBinaryExpression.Operator.ADD;
      case SqlBaseLexer.MINUS:
        return ArithmeticBinaryExpression.Operator.SUBTRACT;
      case SqlBaseLexer.ASTERISK:
        return ArithmeticBinaryExpression.Operator.MULTIPLY;
      case SqlBaseLexer.SLASH:
        return ArithmeticBinaryExpression.Operator.DIVIDE;
      case SqlBaseLexer.PERCENT:
        return ArithmeticBinaryExpression.Operator.MODULUS;
    }

    throw new UnsupportedOperationException("Unsupported operator: " + operator.getText());
  }

  private static Operator getComparisonOperator(Token symbol) {
    switch (symbol.getType()) {
      case SqlBaseLexer.EQ:
        return ComparisonExpression.Operator.EQUAL;
      case SqlBaseLexer.NEQ:
        return ComparisonExpression.Operator.NOT_EQUAL;
      case SqlBaseLexer.LT:
        return ComparisonExpression.Operator.LESS_THAN;
      case SqlBaseLexer.LTE:
        return ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
      case SqlBaseLexer.GT:
        return ComparisonExpression.Operator.GREATER_THAN;
      case SqlBaseLexer.GTE:
        return ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
      case SqlBaseLexer.IS:
        return ComparisonExpression.Operator.EQUAL;
    }

    throw new IllegalArgumentException("Unsupported operator: " + symbol.getText());
  }

  private static IntervalLiteral.IntervalField getIntervalFieldType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.YEAR:
        return IntervalLiteral.IntervalField.YEAR;
      case SqlBaseLexer.MONTH:
        return IntervalLiteral.IntervalField.MONTH;
      case SqlBaseLexer.DAY:
        return IntervalLiteral.IntervalField.DAY;
      case SqlBaseLexer.WEEK:
        return IntervalLiteral.IntervalField.WEEK;
      case SqlBaseLexer.HOUR:
        return IntervalLiteral.IntervalField.HOUR;
      case SqlBaseLexer.MINUTE:
        return IntervalLiteral.IntervalField.MINUTE;
      case SqlBaseLexer.SECOND:
        return IntervalLiteral.IntervalField.SECOND;
    }

    throw new IllegalArgumentException("Unsupported interval field: " + token.getText());
  }

  private static IntervalLiteral.Sign getIntervalSign(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.MINUS:
        return IntervalLiteral.Sign.NEGATIVE;
      case SqlBaseLexer.PLUS:
        return IntervalLiteral.Sign.POSITIVE;
    }

    throw new IllegalArgumentException("Unsupported sign: " + token.getText());
  }

  private static LogicalBinaryExpression.Operator getLogicalBinaryOperator(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.AND:
        return LogicalBinaryExpression.Operator.AND;
      case SqlBaseLexer.OR:
        return LogicalBinaryExpression.Operator.OR;
    }

    throw new IllegalArgumentException("Unsupported operator: " + token.getText());
  }

  private static SortItem.Ordering getOrderingType(Token token) {
    switch (token.getType()) {
      case SqlBaseLexer.ASC:
        return SortItem.Ordering.ASCENDING;
      case SqlBaseLexer.DESC:
        return SortItem.Ordering.DESCENDING;
    }

    throw new IllegalArgumentException("Unsupported ordering: " + token.getText());
  }

  private static void check(boolean condition, String message, ParserRuleContext context) {
    if (!condition) {
      throw parseError(message, context);
    }
  }

  public static NodeLocation getLocation(TerminalNode terminalNode) {
    requireNonNull(terminalNode, "terminalNode is null");
    return getLocation(terminalNode.getSymbol());
  }

  public static NodeLocation getLocation(ParserRuleContext parserRuleContext) {
    requireNonNull(parserRuleContext, "parserRuleContext is null");
    return getLocation(parserRuleContext.getStart());
  }

  public static NodeLocation getLocation(Token token) {
    requireNonNull(token, "token is null");
    return new NodeLocation(token.getLine(), token.getCharPositionInLine());
  }

  private static ParsingException parseError(String message, ParserRuleContext context) {
    return new ParsingException(message, null, context.getStart().getLine(),
        context.getStart().getCharPositionInLine());
  }

  @Override
  public Node visitSingleStatement(SingleStatementContext context) {
    return visit(context.statement());
  }

  @Override
  public Node visitQuery(QueryContext context) {
    Query body = (Query) visit(context.queryNoWith());

    return new Query(
        getLocation(context),
        body.getQueryBody(),
        body.getOrderBy(),
        body.getLimit());
  }

  @Override
  public Node visitQueryNoWith(QueryNoWithContext context) {
    QueryBody term = (QueryBody) visit(context.queryTerm());

    Optional<OrderBy> orderBy = Optional.empty();
    if (context.ORDER() != null) {
      orderBy = Optional
          .of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
    }

    Optional<Limit> limit = Optional.empty();
    Optional<String> limitStr = getTextIfPresent(context.limit);
    if (context.LIMIT() != null && limitStr.isPresent()) {
      limit = Optional.of(new Limit(limitStr.get()));
    }

    if (term instanceof QuerySpecification) {
      // When we have a simple query specification
      // followed by order by limit, fold the order by and limit
      // clauses into the query specification (analyzer/planner
      // expects this structure to resolve references with respect
      // to columns defined in the query specification)
      QuerySpecification query = (QuerySpecification) term;

      return new Query(
          getLocation(context),
          new QuerySpecification(
              getLocation(context),
              query.getSelect(),
              query.getFrom(),
              query.getWhere(),
              query.getGroupBy(),
              query.getHaving(),
              orderBy,
              query.getLimit()),
          Optional.empty(),
          Optional.empty());
    }

    return new Query(
        getLocation(context),
        term,
        orderBy,
        limit);
  }

  @Override
  public Node visitQuerySpecification(QuerySpecificationContext context) {
    Relation from;
    List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);

    List<Relation> relations = visit(context.relation(), Relation.class);
    if (relations.isEmpty()) {
      throw new RuntimeException("FROM required");
    } else {
      // synthesize implicit join nodes
      Iterator<Relation> iterator = relations.iterator();
      Relation relation = iterator.next();

      while (iterator.hasNext()) {
        relation = new Join(getLocation(context), Join.Type.IMPLICIT, relation, iterator.next(),
            Optional.empty());
      }

      from = relation;
    }

    return new QuerySpecification(
        getLocation(context),
        new Select(getLocation(context.SELECT()), isDistinct(context.setQuantifier()), selectItems),
        from,
        visitIfPresent(context.where, Expression.class),
        visitIfPresent(context.groupBy(), GroupBy.class),
        visitIfPresent(context.having, Expression.class),
        Optional.empty(),
        Optional.empty());
  }

  @Override
  public Node visitGroupBy(GroupByContext context) {
    return new GroupBy(getLocation(context),
        (SimpleGroupBy) visit(context.groupingElement()));
  }

  @Override
  public Node visitSingleGroupingSet(SingleGroupingSetContext context) {
    return new SimpleGroupBy(getLocation(context),
        visit(context.groupingSet().expression(), Expression.class));
  }

  @Override
  public Node visitSetOperation(SetOperationContext context) {
    QueryBody left = (QueryBody) visit(context.left);
    QueryBody right = (QueryBody) visit(context.right);

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
        return new Union(getLocation(context.UNION()), ImmutableList.of(left, right), distinct);
      case SqlBaseLexer.INTERSECT:
        return new Intersect(getLocation(context.INTERSECT()), ImmutableList.of(left, right),
            distinct);
      case SqlBaseLexer.EXCEPT:
        return new Except(getLocation(context.EXCEPT()), left, right, distinct);
    }

    throw new IllegalArgumentException("Unsupported set operation: " + context.operator.getText());
  }

  @Override
  public Node visitSelectAll(SelectAllContext context) {

    return new AllColumns(getLocation(context));
  }

  @Override
  public Node visitSelectSingle(SelectSingleContext context) {
    Expression expression = (Expression) visit(context.expression());
    if (expression instanceof Identifier && ((Identifier) expression).getNamePath().getLast().getCanonical()
        .equalsIgnoreCase("*")) {
      return new AllColumns(getLocation(context),
          ((Identifier) expression).getNamePath().getPrefix().get());
    }

    return new SingleColumn(
        getLocation(context),
        (Expression) visit(context.expression()),
        visitIfPresent(context.identifier(), Identifier.class));
  }

  @Override
  public Node visitSubquery(SubqueryContext context) {
    return new TableSubquery(getLocation(context), (Query) visit(context.queryNoWith()));
  }

  // ********************* primary expressions **********************

  @Override
  public Node visitLogicalNot(LogicalNotContext context) {
    return new NotExpression(getLocation(context), (Expression) visit(context.booleanExpression()));
  }

  @Override
  public Node visitLogicalBinary(LogicalBinaryContext context) {
    LogicalBinaryExpression.Operator operator = getLogicalBinaryOperator(context.operator);
    boolean warningForMixedAndOr = false;
    Expression left = (Expression) visit(context.left);
    Expression right = (Expression) visit(context.right);

    if (operator.equals(LogicalBinaryExpression.Operator.OR) &&
        (mixedAndOrOperatorParenthesisCheck(right, context.right,
            LogicalBinaryExpression.Operator.AND) ||
            mixedAndOrOperatorParenthesisCheck(left, context.left,
                LogicalBinaryExpression.Operator.AND))) {
      warningForMixedAndOr = true;
    }

    if (operator.equals(LogicalBinaryExpression.Operator.AND) &&
        (mixedAndOrOperatorParenthesisCheck(right, context.right,
            LogicalBinaryExpression.Operator.OR) ||
            mixedAndOrOperatorParenthesisCheck(left, context.left,
                LogicalBinaryExpression.Operator.OR))) {
      warningForMixedAndOr = true;
    }

    if (warningForMixedAndOr) {
      warningConsumer.accept(new ParsingWarning(
          "The Query contains OR and AND operator without proper parenthesis. "
              + "Make sure the operators are guarded by parenthesis in order "
              + "to fetch logically correct results",
          context.getStart().getLine(), context.getStart().getCharPositionInLine()));
    }

    return new LogicalBinaryExpression(
        getLocation(context.operator),
        operator,
        left,
        right);
  }

  @Override
  public Node visitJoinRelation(JoinRelationContext context) {
    Relation left = (Relation) visit(context.left);
    Relation right;

    if (context.CROSS() != null) {
      right = (Relation) visit(context.right);
      return new Join(getLocation(context), Join.Type.CROSS, left, right, Optional.empty());
    }

    JoinOn criteria = null;
    right = (Relation) visit(context.rightRelation);
    if (context.joinCriteria() != null && context.joinCriteria().ON() != null) {
      criteria = new JoinOn(getLocation(context),
          (Expression) visit(context.joinCriteria().booleanExpression()));
    }

    return new Join(getLocation(context), toJoinType(context.joinType()), left, right,
        Optional.ofNullable(criteria));
  }

  public Join.Type toJoinType(JoinTypeContext joinTypeContext) {

    Join.Type joinType;
    if (joinTypeContext.LEFT() != null) {
      joinType = Join.Type.LEFT;
    } else if (joinTypeContext.RIGHT() != null) {
      joinType = Join.Type.RIGHT;
    } else if (joinTypeContext.FULL() != null) {
      joinType = Join.Type.FULL;
    } else {
      joinType = Join.Type.INNER;
    }

    return joinType;
  }

  @Override
  public Node visitTableName(TableNameContext context) {
    Optional<Name> alias = Optional.empty();
    if (context.identifier() != null) {
      alias = Optional.of(((Identifier) visit(context.identifier())).getNamePath().getFirst());
    }

    return new TableNode(getLocation(context), getNamePath(context.qualifiedName()), alias);
  }

  @Override
  public Node visitInlineJoinBody(InlineJoinBodyContext ctx) {
    return visit(ctx);
  }

  @Override
  public Node visitExpression(ExpressionContext ctx) {
    return visit(ctx.booleanExpression());
  }

  @Override
  public Node visitValueExpressionDefault(ValueExpressionDefaultContext ctx) {
    return visit(ctx.primaryExpression().get(0));
  }

  @Override
  public Node visitPredicated(PredicatedContext context) {
    if (context.predicate() != null) {
      return visit(context.predicate());
    }

    return visit(context.valueExpression);
  }

  @Override
  public Node visitComparison(ComparisonContext context) {
    return new ComparisonExpression(
        getLocation(context.comparisonOperator()),
        getComparisonOperator(
            ((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
        (Expression) visit(context.value),
        (Expression) visit(context.right));
  }

  @Override
  public Node visitBetween(BetweenContext context) {
    Expression expression = new BetweenPredicate(
        getLocation(context),
        (Expression) visit(context.value),
        (Expression) visit(context.lower),
        (Expression) visit(context.upper));

    if (context.NOT() != null) {
      expression = new NotExpression(getLocation(context), expression);
    }

    return expression;
  }

  @Override
  public Node visitNullPredicate(NullPredicateContext context) {
    Expression child = (Expression) visit(context.value);

    if (context.NOT() == null) {
      return new IsNullPredicate(getLocation(context), child);
    }

    return new IsNotNullPredicate(getLocation(context), child);
  }

  @Override
  public Node visitInList(InListContext context) {
    Expression result = new InPredicate(
        getLocation(context),
        (Expression) visit(context.value),
        new InListExpression(getLocation(context), visit(context.expression(), Expression.class)));

    if (context.NOT() != null) {
      result = new NotExpression(getLocation(context), result);
    }

    return result;
  }

  @Override
  public Node visitInSubquery(InSubqueryContext context) {
    Expression result = new InPredicate(
        getLocation(context),
        (Expression) visit(context.value),
        new SubqueryExpression(getLocation(context), (Query) visit(context.query())));

    if (context.NOT() != null) {
      result = new NotExpression(getLocation(context), result);
    }

    return result;
  }
//
//  @Override
//  public Node visitExists(SqlBaseParser.ExistsContext context) {
//    return new ExistsPredicate(getLocation(context),
//        new SubqueryExpression(getLocation(context), (Query) visit(context.query())));
//  }

  @Override
  public Node visitArithmeticUnary(ArithmeticUnaryContext context) {
    Expression child = (Expression) visit(context.valueExpression());

    switch (context.operator.getType()) {
      case SqlBaseLexer.MINUS:
        return ArithmeticUnaryExpression.negative(getLocation(context), child);
      case SqlBaseLexer.PLUS:
        return ArithmeticUnaryExpression.positive(getLocation(context), child);
      default:
        throw new UnsupportedOperationException("Unsupported sign: " + context.operator.getText());
    }
  }

  @Override
  public Node visitArithmeticBinary(ArithmeticBinaryContext context) {
    return new ArithmeticBinaryExpression(
        getLocation(context.operator),
        getArithmeticBinaryOperator(context.operator),
        (Expression) visit(context.left),
        (Expression) visit(context.right));
  }

  @Override
  public Node visitParenthesizedExpression(ParenthesizedExpressionContext context) {
    return visit(context.expression());
  }

  @Override
  public Node visitCast(CastContext context) {
    return new Cast(getLocation(context), (Expression) visit(context.expression()),
        getType(context.type()), false);
  }

  @Override
  public Node visitQualifiedName(QualifiedNameContext ctx) {
    return new Identifier(getLocation(ctx), NamePath.parse(ctx.getText()));
  }

  @Override
  public Node visitSubqueryExpression(SubqueryExpressionContext context) {
    return new SubqueryExpression(getLocation(context), (Query) visit(context.query()));
  }

  @Override
  public Node visitColumnReference(ColumnReferenceContext context) {
    return visit(context.qualifiedName());
  }

  @Override
  public Node visitSimpleCase(SimpleCaseContext context) {
    return new SimpleCaseExpression(
        getLocation(context),
        visit(context.whenClause(), WhenClause.class),
        visitIfPresent(context.elseExpression, Expression.class));
  }

  @Override
  public Node visitWhenClause(WhenClauseContext context) {
    return new WhenClause(getLocation(context), (Expression) visit(context.condition),
        (Expression) visit(context.result));
  }

  @Override
  public Node visitFunctionCall(FunctionCallContext context) {
    boolean distinct = isDistinct(context.setQuantifier());

    return new FunctionCall(
        getLocation(context),
        getNamePath(context.qualifiedName()),
        visit(context.expression(), Expression.class),
        distinct);
  }

  // ***************** helpers *****************

  @Override
  public Node visitSortItem(SortItemContext context) {
    return new SortItem(
        getLocation(context),
        (Expression) visit(context.expression()),
        Optional.ofNullable(context.ordering)
            .map(AstBuilder::getOrderingType)
            .orElse(SortItem.Ordering.ASCENDING));
  }

  @Override
  public Node visitUnquotedIdentifier(UnquotedIdentifierContext context) {
    return new Identifier(getLocation(context), NamePath.parse(context.getText()));
  }

  @Override
  public Node visitQuotedIdentifier(QuotedIdentifierContext context) {
    String token = context.getText();
    String identifier = token.substring(1, token.length() - 1)
        .replace("\"\"", "\"");

    return new Identifier(getLocation(context), NamePath.of(identifier));
  }

  @Override
  public Node visitNullLiteral(NullLiteralContext context) {
    return new NullLiteral(getLocation(context));
  }

  @Override
  public Node visitBasicStringLiteral(BasicStringLiteralContext context) {
    return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
  }

  @Override
  public Node visitUnicodeStringLiteral(UnicodeStringLiteralContext context) {
    return new StringLiteral(getLocation(context), decodeUnicodeLiteral(context));
  }

  @Override
  public Node visitIntegerLiteral(IntegerLiteralContext context) {
    return new LongLiteral(getLocation(context), context.getText());
  }

  @Override
  public Node visitDecimalLiteral(DecimalLiteralContext context) {
    switch (parsingOptions.getDecimalLiteralTreatment()) {
      case AS_DOUBLE:
        return new DoubleLiteral(getLocation(context), context.getText());
      case AS_DECIMAL:
        return new DecimalLiteral(getLocation(context), context.getText());
      case REJECT:
        throw new ParsingException("Unexpected decimal literal: " + context.getText());
    }
    throw new AssertionError("Unreachable");
  }

  @Override
  public Node visitDoubleLiteral(DoubleLiteralContext context) {
    return new DoubleLiteral(getLocation(context), context.getText());
  }

  @Override
  public Node visitBooleanValue(BooleanValueContext context) {
    return new BooleanLiteral(getLocation(context), context.getText());
  }

  @Override
  public Node visitInterval(IntervalContext context) {
    return new IntervalLiteral(
        Optional.of(getLocation(context)),
        (Expression) context.expression().accept(this),
        Optional.ofNullable(context.sign)
            .map(AstBuilder::getIntervalSign)
            .orElse(IntervalLiteral.Sign.POSITIVE),
        getIntervalFieldType((Token) context.intervalField().getChild(0).getPayload()));
  }

  @Override
  public Node visitScript(ScriptContext ctx) {
    return new ScriptNode(
        getLocation(ctx),
        visit(ctx.statement(), Node.class));
  }

  @Override
  public Node visitImportStatement(ImportStatementContext ctx) {
    return visit(ctx.importDefinition());
  }

  @Override
  public Node visitImportDefinition(ImportDefinitionContext ctx) {
    Optional<Identifier> alias = Optional.ofNullable(
        ctx.alias == null ? null : (Identifier) visit(ctx.alias));
    return new ImportDefinition(getLocation(ctx), getNamePath(ctx.qualifiedName()), alias);
  }

  @Override
  public Node visitAssign(AssignContext context) {
    return context.assignment().accept(this);
  }

  @Override
  public Node visitDistinctAssignment(DistinctAssignmentContext ctx) {
    NamePath name = getNamePath(ctx.qualifiedName());
    return new DistinctAssignment(
        Optional.of(getLocation(ctx)),
        name,
        Name.system(visit(ctx.table).toString()),
        ctx.identifier() == null ? List.of() :
            ctx.identifier().stream().skip(1)
                .map(s -> Name.system(visit(s).toString()))
                .collect(toList()),
        ctx.sortItem() == null ? List.of() : ctx.sortItem().stream()
            .map(s -> (SortItem) s.accept(this)).collect(toList())

    );
  }

  @Override
  public Node visitJoinAssignment(JoinAssignmentContext ctx) {
    NamePath name = getNamePath(ctx.qualifiedName());
    String query = "";

    return new JoinAssignment(Optional.of(getLocation(ctx)), name,
        query,
        (JoinDeclaration) visit(ctx.inlineJoin()));
  }

  @Override
  public Node visitInlineJoin(InlineJoinContext ctx) {
    Optional<OrderBy> orderBy = Optional.empty();
    if (ctx.ORDER() != null) {
      orderBy = Optional
          .of(new OrderBy(getLocation(ctx.ORDER()), visit(ctx.sortItem(), SortItem.class)));
    }

    Relation current = new TableNode(Optional.empty(), Name.SELF_IDENTIFIER.toNamePath(),
        Optional.empty());
    for (InlineJoinBodyContext inline : ctx.inlineJoinBody()) {
      JoinOn criteria = null;
      if (inline.joinCriteria() != null && inline.joinCriteria().ON() != null) {
        criteria = new JoinOn(getLocation(inline),
            (Expression) visit(inline.joinCriteria().booleanExpression()));
      }

      current = new Join(
          Optional.of(getLocation(inline)),
          toJoinType(inline.joinType()),
          current,
          (Relation) visit(inline.relationPrimary()),
          Optional.ofNullable(criteria)
      );
    }

    return new JoinDeclaration(
        Optional.of(getLocation(ctx)),
        current,
        orderBy,
        ctx.limit == null || ctx.limit.getText().equalsIgnoreCase("ALL") ? Optional.empty() :
            Optional.of(new Limit(ctx.limit.getText())),
        ctx.inv == null ? Optional.empty() :
            Optional.of(((Identifier) visit(ctx.inv)).getNamePath().getFirst())
    );
  }

  @Override
  public Node visitQueryAssign(QueryAssignContext ctx) {
    Interval interval = new Interval(
        ctx.query().start.getStartIndex(),
        ctx.query().stop.getStopIndex());
    String query = ctx.query().start.getInputStream().getText(interval);
    return new QueryAssignment(Optional.of(getLocation(ctx)), getNamePath(ctx.qualifiedName()),
        (Query) visitQuery(ctx.query()), query);
  }

  @Override
  public Node visitExpressionAssign(ExpressionAssignContext ctx) {
    NamePath name = getNamePath(ctx.qualifiedName());
    Interval interval = new Interval(
        ctx.expression().start.getStartIndex(),
        ctx.expression().stop.getStopIndex());
    String expression = ctx.expression().start.getInputStream().getText(interval);

    return new ExpressionAssignment(Optional.of(getLocation(ctx)), name,
        (Expression) visitExpression(ctx.expression()), expression);
  }

  @Override
  public Node visitBackQuotedIdentifier(BackQuotedIdentifierContext ctx) {
    return new Identifier(getLocation(ctx), NamePath.parse(ctx.getText()));
  }

  @Override
  protected Node defaultResult() {
    return null;
  }

  @Override
  protected Node aggregateResult(Node aggregate, Node nextResult) {
    if (nextResult == null) {
      throw new UnsupportedOperationException("not yet implemented:" + aggregate);
    }

    if (aggregate == null) {
      return nextResult;
    }

    throw new UnsupportedOperationException("not yet implemented");
  }
//
//  @Override
//  public Node visitIsEmpty(IsEmptyContext ctx) {
//    return new IsEmpty(Optional.of(getLocation(ctx)),ctx.NOT() == null);
//  }

  @Override
  public Node visitCreateSubscription(CreateSubscriptionContext ctx) {
    return new CreateSubscription(
        Optional.of(getLocation(ctx)),
        SubscriptionType.valueOf(ctx.subscriptionType().getText()),
        getNamePath(ctx.qualifiedName()),
        (Query) visit(ctx.query())
    );
  }

  @Override
  public Node visitInRelation(InRelationContext context) {
    Expression result = new InPredicate(
        getLocation(context),
        (Expression) visit(context.value),
        null //todo figure out how to handle QualifiedName as an IN expression
    );

    if (context.NOT() != null) {
      result = new NotExpression(getLocation(context), result);
    }

    return result;
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

  private NamePath getNamePath(QualifiedNameContext context) {
    List<Name> parts = visit(context.identifier(), Identifier.class).stream()
        .map(Identifier::getNamePath) // TODO: preserve quotedness
        .flatMap(e -> Arrays.stream(e.getNames()))
        .collect(Collectors.toList());
    if (context.all != null) {
      parts = new ArrayList<>(parts);
      parts.add(Name.of("*", NameCanonicalizer.LOWERCASE_ENGLISH));
    }

    return NamePath.of(parts);
  }

  private String getType(TypeContext type) {
    if (type.baseType() != null) {
      String signature = type.baseType().getText();
      if (!type.typeParameter().isEmpty()) {
        String typeParameterSignature = type
            .typeParameter()
            .stream()
            .map(this::typeParameterToString)
            .collect(Collectors.joining(","));
        signature += "(" + typeParameterSignature + ")";
      }
      return signature;
    }
//
//    if (type.ARRAY() != null) {
//      return "ARRAY(" + getType(type.type(0)) + ")";
//    }
//
//    if (type.MAP() != null) {
//      return "MAP(" + getType(type.type(0)) + "," + getType(type.type(1)) + ")";
//    }
//
//    if (type.ROW() != null) {
//      StringBuilder builder = new StringBuilder("(");
//      for (int i = 0; i < type.identifier().size(); i++) {
//        if (i != 0) {
//          builder.append(",");
//        }
//        builder.append(visit(type.identifier(i)))
//            .append(" ")
//            .append(getType(type.type(i)));
//      }
//      builder.append(")");
//      return "ROW" + builder;
//    }

    if (type.INTERVAL() != null) {
      return "INTERVAL " + getIntervalFieldType((Token) type.from.getChild(0).getPayload()) +
          " TO " + getIntervalFieldType((Token) type.to.getChild(0).getPayload());
    }

    throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
  }

  private String typeParameterToString(TypeParameterContext typeParameter) {
    if (typeParameter.INTEGER_VALUE() != null) {
      return typeParameter.INTEGER_VALUE().toString();
    }
    if (typeParameter.type() != null) {
      return getType(typeParameter.type());
    }
    throw new IllegalArgumentException("Unsupported typeParameter: " + typeParameter.getText());
  }

  private boolean mixedAndOrOperatorParenthesisCheck(Expression expression,
      BooleanExpressionContext node, LogicalBinaryExpression.Operator operator) {
    if (expression instanceof LogicalBinaryExpression) {
      if (((LogicalBinaryExpression) expression).getOperator().equals(operator)) {
        if (node.children.get(0) instanceof ValueExpressionDefaultContext) {
          return !(((PredicatedContext) node).valueExpression().getChild(0) instanceof
              ParenthesizedExpressionContext);
        } else {
          return true;
        }
      }
    }
    return false;
  }

  private enum UnicodeDecodeState {
    EMPTY,
    ESCAPED,
    UNICODE_SEQUENCE
  }
}
