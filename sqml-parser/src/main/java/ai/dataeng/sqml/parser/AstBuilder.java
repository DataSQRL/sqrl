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
package ai.dataeng.sqml.parser;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import ai.dataeng.sqml.parser.SqlBaseParser.*;
import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.AllColumns;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.ArithmeticUnaryExpression;
import ai.dataeng.sqml.tree.Assign;
import ai.dataeng.sqml.tree.Assignment;
import ai.dataeng.sqml.tree.BetweenPredicate;
import ai.dataeng.sqml.tree.BinaryLiteral;
import ai.dataeng.sqml.tree.BooleanLiteral;
import ai.dataeng.sqml.tree.Cast;
import ai.dataeng.sqml.tree.CharLiteral;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.DecimalLiteral;
import ai.dataeng.sqml.tree.DereferenceExpression;
import ai.dataeng.sqml.tree.DoubleLiteral;
import ai.dataeng.sqml.tree.Except;
import ai.dataeng.sqml.tree.ExistsPredicate;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GenericLiteral;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.GroupingElement;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Import;
import ai.dataeng.sqml.tree.Import.ImportType;
import ai.dataeng.sqml.tree.InListExpression;
import ai.dataeng.sqml.tree.InPredicate;
import ai.dataeng.sqml.tree.Intersect;
import ai.dataeng.sqml.tree.IntervalLiteral;
import ai.dataeng.sqml.tree.IsNotNullPredicate;
import ai.dataeng.sqml.tree.IsNullPredicate;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.LikePredicate;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.NaturalJoin;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeLocation;
import ai.dataeng.sqml.tree.NotExpression;
import ai.dataeng.sqml.tree.NullLiteral;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Parameter;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.QueryBody;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.JoinSubexpression;
import ai.dataeng.sqml.tree.Row;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.IsEmpty;
import ai.dataeng.sqml.tree.SimpleCaseExpression;
import ai.dataeng.sqml.tree.SimpleGroupBy;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.StringLiteral;
import ai.dataeng.sqml.tree.SubqueryExpression;
import ai.dataeng.sqml.tree.SubscriptionType;
import ai.dataeng.sqml.tree.Table;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.TimeLiteral;
import ai.dataeng.sqml.tree.TimestampLiteral;
import ai.dataeng.sqml.tree.TraversalJoin;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
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

  private static String decodeUnicodeLiteral(SqlBaseParser.UnicodeStringLiteralContext context) {
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

  private static boolean isDistinct(SqlBaseParser.SetQuantifierContext setQuantifier) {
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

  private static ComparisonExpression.Operator getComparisonOperator(Token symbol) {
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
  public Node visitSingleStatement(SqlBaseParser.SingleStatementContext context) {
    return visit(context.statement());
  }

  @Override
  public Node visitQuery(SqlBaseParser.QueryContext context) {
    Query body = (Query) visit(context.queryNoWith());

    return new Query(
        getLocation(context),
        body.getQueryBody(),
        body.getOrderBy(),
        body.getLimit());
  }

  @Override
  public Node visitQueryNoWith(SqlBaseParser.QueryNoWithContext context) {
    QueryBody term = (QueryBody) visit(context.queryTerm());

    Optional<OrderBy> orderBy = Optional.empty();
    if (context.ORDER() != null) {
      orderBy = Optional
          .of(new OrderBy(getLocation(context.ORDER()), visit(context.sortItem(), SortItem.class)));
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
              getTextIfPresent(context.limit)),
          Optional.empty(),
          Optional.empty());
    }

    return new Query(
        getLocation(context),
        term,
        orderBy,
        getTextIfPresent(context.limit));
  }

  @Override
  public Node visitQuerySpecification(SqlBaseParser.QuerySpecificationContext context) {
    Optional<Relation> from = Optional.empty();
    List<SelectItem> selectItems = visit(context.selectItem(), SelectItem.class);

    List<Relation> relations = visit(context.relation(), Relation.class);
    if (!relations.isEmpty()) {
      // synthesize implicit join nodes
      Iterator<Relation> iterator = relations.iterator();
      Relation relation = iterator.next();

      while (iterator.hasNext()) {
        relation = new Join(getLocation(context), Join.Type.IMPLICIT, relation, iterator.next(),
            Optional.empty());
      }

      from = Optional.of(relation);
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
  public Node visitGroupBy(SqlBaseParser.GroupByContext context) {
    return new GroupBy(getLocation(context),
        visit(context.groupingElement(), GroupingElement.class));
  }

  @Override
  public Node visitSingleGroupingSet(SqlBaseParser.SingleGroupingSetContext context) {
    return new SimpleGroupBy(getLocation(context),
        visit(context.groupingSet().expression(), Expression.class));
  }

  @Override
  public Node visitSetOperation(SqlBaseParser.SetOperationContext context) {
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
  public Node visitSelectAll(SqlBaseParser.SelectAllContext context) {
    if (context.qualifiedName() != null) {
      return new AllColumns(getLocation(context), getQualifiedName(context.qualifiedName()));
    }

    return new AllColumns(getLocation(context));
  }

  @Override
  public Node visitSelectSingle(SqlBaseParser.SelectSingleContext context) {
    return new SingleColumn(
        getLocation(context),
        (Expression) visit(context.expression()),
        visitIfPresent(context.identifier(), Identifier.class));
  }

  @Override
  public Node visitSubquery(SqlBaseParser.SubqueryContext context) {
    return new TableSubquery(getLocation(context), (Query) visit(context.queryNoWith()));
  }

  // ********************* primary expressions **********************

  @Override
  public Node visitLogicalNot(SqlBaseParser.LogicalNotContext context) {
    return new NotExpression(getLocation(context), (Expression) visit(context.booleanExpression()));
  }

  @Override
  public Node visitLogicalBinary(SqlBaseParser.LogicalBinaryContext context) {
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
  public Node visitJoinRelation(SqlBaseParser.JoinRelationContext context) {
    Relation left = (Relation) visit(context.left);
    Relation right;

    if (context.CROSS() != null) {
      right = (Relation) visit(context.right);
      return new Join(getLocation(context), Join.Type.CROSS, left, right, Optional.empty());
    }

    JoinCriteria criteria = null;
    if (context.NATURAL() != null) {
      right = (Relation) visit(context.right);
      criteria = new NaturalJoin(getLocation(context));
    } else {
      right = (Relation) visit(context.rightRelation);
      if (context.joinCriteria() != null && context.joinCriteria().ON() != null) {
        criteria = new JoinOn(getLocation(context), (Expression) visit(context.joinCriteria().booleanExpression()));
      }
    }

    Join.Type joinType;
    if (context.joinType().LEFT() != null) {
      joinType = Join.Type.LEFT;
    } else if (context.joinType().RIGHT() != null) {
      joinType = Join.Type.RIGHT;
    } else if (context.joinType().FULL() != null) {
      joinType = Join.Type.FULL;
    } else {
      joinType = Join.Type.INNER;
    }

    return new Join(getLocation(context), joinType, left, right, Optional.ofNullable(criteria));
  }

  @Override
  public Node visitAliasedRelation(SqlBaseParser.AliasedRelationContext context) {
    Relation child = (Relation) visit(context.relationPrimary());

    if (context.identifier() == null) {
      return child;
    }

    List<Identifier> aliases = null;
    if (context.columnAliases() != null) {
      aliases = visit(context.columnAliases().identifier(), Identifier.class);
    }

    return new AliasedRelation(getLocation(context), child,
        (Identifier) visit(context.identifier()), aliases);
  }

  @Override
  public Node visitTableName(SqlBaseParser.TableNameContext context) {
    return new Table(getLocation(context), getQualifiedName(context.qualifiedName()));
  }

  @Override
  public Node visitSubqueryRelation(SqlBaseParser.SubqueryRelationContext context) {
    return new TableSubquery(getLocation(context), (Query) visit(context.query()));
  }

  @Override
  public Node visitExpression(ExpressionContext ctx) {
    return visit(ctx.booleanExpression());
  }

  @Override
  public Node visitParenthesizedRelation(SqlBaseParser.ParenthesizedRelationContext context) {
    return visit(context.relation());
  }

  @Override
  public Node visitValueExpressionDefault(ValueExpressionDefaultContext ctx) {
    return visit(ctx.primaryExpression().get(0));
  }

  @Override
  public Node visitPredicated(SqlBaseParser.PredicatedContext context) {
    if (context.predicate() != null) {
      return visit(context.predicate());
    }

    return visit(context.valueExpression);
  }

  @Override
  public Node visitComparison(SqlBaseParser.ComparisonContext context) {
    return new ComparisonExpression(
        getLocation(context.comparisonOperator()),
        getComparisonOperator(
            ((TerminalNode) context.comparisonOperator().getChild(0)).getSymbol()),
        (Expression) visit(context.value),
        (Expression) visit(context.right));
  }

  @Override
  public Node visitDistinctFrom(SqlBaseParser.DistinctFromContext context) {
    Expression expression = new ComparisonExpression(
        getLocation(context),
        ComparisonExpression.Operator.IS_DISTINCT_FROM,
        (Expression) visit(context.value),
        (Expression) visit(context.right));

    if (context.NOT() != null) {
      expression = new NotExpression(getLocation(context), expression);
    }

    return expression;
  }

  @Override
  public Node visitBetween(SqlBaseParser.BetweenContext context) {
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
  public Node visitNullPredicate(SqlBaseParser.NullPredicateContext context) {
    Expression child = (Expression) visit(context.value);

    if (context.NOT() == null) {
      return new IsNullPredicate(getLocation(context), child);
    }

    return new IsNotNullPredicate(getLocation(context), child);
  }

  @Override
  public Node visitLike(SqlBaseParser.LikeContext context) {
    Expression result = new LikePredicate(
        getLocation(context),
        (Expression) visit(context.value),
        (Expression) visit(context.pattern));

    if (context.NOT() != null) {
      result = new NotExpression(getLocation(context), result);
    }

    return result;
  }

  @Override
  public Node visitInList(SqlBaseParser.InListContext context) {
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
  public Node visitInSubquery(SqlBaseParser.InSubqueryContext context) {
    Expression result = new InPredicate(
        getLocation(context),
        (Expression) visit(context.value),
        new SubqueryExpression(getLocation(context), (Query) visit(context.query())));

    if (context.NOT() != null) {
      result = new NotExpression(getLocation(context), result);
    }

    return result;
  }

  @Override
  public Node visitExists(SqlBaseParser.ExistsContext context) {
    return new ExistsPredicate(getLocation(context),
        new SubqueryExpression(getLocation(context), (Query) visit(context.query())));
  }

  @Override
  public Node visitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext context) {
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
  public Node visitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext context) {
    return new ArithmeticBinaryExpression(
        getLocation(context.operator),
        getArithmeticBinaryOperator(context.operator),
        (Expression) visit(context.left),
        (Expression) visit(context.right));
  }

  @Override
  public Node visitConcatenation(SqlBaseParser.ConcatenationContext context) {
    return new FunctionCall(
        getLocation(context.CONCAT()),
        QualifiedName.of("concat"), ImmutableList.of(
        (Expression) visit(context.left),
        (Expression) visit(context.right)));
  }

  @Override
  public Node visitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext context) {
    return visit(context.expression());
  }

  @Override
  public Node visitRowConstructor(SqlBaseParser.RowConstructorContext context) {
    return new Row(getLocation(context), visit(context.expression(), Expression.class));
  }

  @Override
  public Node visitCast(SqlBaseParser.CastContext context) {
    return new Cast(getLocation(context), (Expression) visit(context.expression()),
        getType(context.type()), false);
  }

  @Override
  public Node visitQualifiedName(QualifiedNameContext ctx) {
    return new Identifier(ctx.getText());
  }

  @Override
  public Node visitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext context) {
    return new SubqueryExpression(getLocation(context), (Query) visit(context.query()));
  }

  @Override
  public Node visitDereference(SqlBaseParser.DereferenceContext context) {
    return new DereferenceExpression(
        getLocation(context),
        (Expression) visit(context.base),
        (Identifier) visit(context.fieldName));
  }

  @Override
  public Node visitColumnReference(SqlBaseParser.ColumnReferenceContext context) {
    return visit(context.qualifiedName());
  }

  @Override
  public Node visitSimpleCase(SqlBaseParser.SimpleCaseContext context) {
    return new SimpleCaseExpression(
        getLocation(context),
        visit(context.whenClause(), WhenClause.class),
        visitIfPresent(context.elseExpression, Expression.class));
  }

  @Override
  public Node visitWhenClause(SqlBaseParser.WhenClauseContext context) {
    return new WhenClause(getLocation(context), (Expression) visit(context.condition),
        (Expression) visit(context.result));
  }

  @Override
  public Node visitFunctionCall(SqlBaseParser.FunctionCallContext context) {
    return new FunctionCall(
        getLocation(context),
        getQualifiedName(context.qualifiedName()),
        visit(context.expression(), Expression.class));
  }

  // ***************** helpers *****************

  @Override
  public Node visitSortItem(SqlBaseParser.SortItemContext context) {
    return new SortItem(
        getLocation(context),
        (Expression) visit(context.expression()),
        Optional.ofNullable(context.ordering)
            .map(AstBuilder::getOrderingType)
            .orElse(SortItem.Ordering.ASCENDING));
  }

  @Override
  public Node visitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context) {
    return new Identifier(getLocation(context), context.getText(), false);
  }

  @Override
  public Node visitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext context) {
    String token = context.getText();
    String identifier = token.substring(1, token.length() - 1)
        .replace("\"\"", "\"");

    return new Identifier(getLocation(context), identifier, true);
  }

  @Override
  public Node visitNullLiteral(SqlBaseParser.NullLiteralContext context) {
    return new NullLiteral(getLocation(context));
  }

  @Override
  public Node visitBasicStringLiteral(SqlBaseParser.BasicStringLiteralContext context) {
    return new StringLiteral(getLocation(context), unquote(context.STRING().getText()));
  }

  @Override
  public Node visitUnicodeStringLiteral(SqlBaseParser.UnicodeStringLiteralContext context) {
    return new StringLiteral(getLocation(context), decodeUnicodeLiteral(context));
  }

  @Override
  public Node visitBinaryLiteral(SqlBaseParser.BinaryLiteralContext context) {
    String raw = context.BINARY_LITERAL().getText();
    return new BinaryLiteral(getLocation(context), unquote(raw.substring(1)));
  }

  @Override
  public Node visitTypeConstructor(SqlBaseParser.TypeConstructorContext context) {
    String value = ((StringLiteral) visit(context.string())).getValue();

    String type = context.identifier().getText();
    if (type.equalsIgnoreCase("time")) {
      return new TimeLiteral(getLocation(context), value);
    }
    if (type.equalsIgnoreCase("timestamp")) {
      return new TimestampLiteral(getLocation(context), value);
    }
    if (type.equalsIgnoreCase("decimal")) {
      return new DecimalLiteral(getLocation(context), value);
    }
    if (type.equalsIgnoreCase("char")) {
      return new CharLiteral(getLocation(context), value);
    }

    return new GenericLiteral(getLocation(context), type, value);
  }

  @Override
  public Node visitIntegerLiteral(SqlBaseParser.IntegerLiteralContext context) {
    return new LongLiteral(getLocation(context), context.getText());
  }

  @Override
  public Node visitDecimalLiteral(SqlBaseParser.DecimalLiteralContext context) {
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
  public Node visitDoubleLiteral(SqlBaseParser.DoubleLiteralContext context) {
    return new DoubleLiteral(getLocation(context), context.getText());
  }

  @Override
  public Node visitBooleanValue(SqlBaseParser.BooleanValueContext context) {
    return new BooleanLiteral(getLocation(context), context.getText());
  }

  @Override
  public Node visitInterval(SqlBaseParser.IntervalContext context) {
    return new IntervalLiteral(
        getLocation(context),
        "1", //todo fix me
//        ((StringLiteral) visit(context.string())).getValue(),
        Optional.ofNullable(context.sign)
            .map(AstBuilder::getIntervalSign)
            .orElse(IntervalLiteral.Sign.POSITIVE),
        getIntervalFieldType((Token) context.from.getChild(0).getPayload()),
        Optional.ofNullable(context.to)
            .map((x) -> x.getChild(0).getPayload())
            .map(Token.class::cast)
            .map(AstBuilder::getIntervalFieldType));
  }

  @Override
  public Node visitParameter(SqlBaseParser.ParameterContext context) {
    Parameter parameter = new Parameter(getLocation(context), parameterPosition);
    parameterPosition++;
    return parameter;
  }

  @Override
  public Node visitScript(ScriptContext ctx) {
    return new Script(
        getLocation(ctx),
        visit(ctx.statement(), Node.class));
  }

  @Override
  public Node visitImportStatement(ImportStatementContext ctx) {
    Optional<ImportType> type = (ctx.importType == null) ?
        Optional.empty() : Optional.of(ImportType.valueOf(ctx.importType.getText().toUpperCase(Locale.ROOT)));

    return new Import(getLocation(ctx), type, getQualifiedName(ctx.qualifiedName()));
  }

  @Override
  public Node visitAssign(AssignContext context) {
    QualifiedName name = getQualifiedName(context.qualifiedName());
//
//    if (context.assignment() instanceof RelationAssignContext) {
//      RelationAssignContext ctx = (RelationAssignContext)context.assignment();
//      RelationshipJoinContext rctx = ctx.relationshipJoin();
//      return new CreateRelationship(
//          Optional.of(getLocation(ctx)),
//          name,
//          getQualifiedName(rctx.table),
//          visit(rctx.expression()),
//          getQualifiedNameIfPresent(rctx.inv),
//          getTextIfPresent(rctx.limit)
//      );
//    }

    return new Assign(getLocation(context), name, (Assignment)visit(context.assignment()));
  }

  @Override
  public Node visitJoinSubexpression(JoinSubexpressionContext ctx) {
    return new JoinSubexpression(
        Optional.of(getLocation(ctx)),
        new TraversalJoin(
            Optional.of(getLocation(ctx)),
            getQualifiedName(ctx.table),
            ctx.identifier() == null ? Optional.empty() :
                Optional.of((Identifier)visit(ctx.identifier())),
            (Expression)visit(ctx.expression()),
            ctx.inv == null ? Optional.empty() :
                Optional.of(getQualifiedName(ctx.inv)),
            ctx.limit == null || ctx.limit.getText().equalsIgnoreCase("ALL") ? Optional.empty() :
                Optional.of(Integer.parseInt(ctx.limit.getText()))
        )
    );
  }

  @Override
  public Node visitQueryAssign(QueryAssignContext ctx) {
    return new QueryAssignment(Optional.of(getLocation(ctx)),
        (Query)visitQuery(ctx.query()));
  }

  @Override
  public Node visitExpressionAssign(ExpressionAssignContext ctx) {
    return new ExpressionAssignment(Optional.of(getLocation(ctx)),
        (Expression)visitExpression(ctx.expression()));
  }

  @Override
  public Node visitCreateRelationship(CreateRelationshipContext ctx) {
    JoinSubexpressionContext rctx = ctx.joinSubexpression();
    return new CreateRelationship(
        Optional.of(getLocation(ctx)),
        getQualifiedName(ctx.qualifiedName()),
        getQualifiedName(rctx.table),
        visit(rctx.expression()),
        getQualifiedNameIfPresent(rctx.inv),
        getTextIfPresent(rctx.limit)
    );
  }

  @Override
  public Node visitBackQuotedIdentifier(BackQuotedIdentifierContext ctx) {
    return new Identifier(ctx.getText());
  }

  @Override
  protected Node defaultResult() {
    return null;
  }

  @Override
  protected Node aggregateResult(Node aggregate, Node nextResult) {
    if (nextResult == null) {
      throw new UnsupportedOperationException("not yet implemented");
    }

    if (aggregate == null) {
      return nextResult;
    }

    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public Node visitIsEmpty(IsEmptyContext ctx) {
    return new IsEmpty(Optional.of(getLocation(ctx)),ctx.NOT() == null);
  }

  @Override
  public Node visitCreateSubscription(CreateSubscriptionContext ctx) {
    return new CreateSubscription(
        Optional.of(getLocation(ctx)),
        SubscriptionType.valueOf(ctx.subscriptionType().getText()),
        getQualifiedName(ctx.qualifiedName()),
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

  @Override
  public Node visitJoinSubexpr(JoinSubexprContext ctx) {
    return visit(ctx.joinSubexpression());
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

  private QualifiedName getQualifiedName(SqlBaseParser.QualifiedNameContext context) {
    List<String> parts = visit(context.identifier(), Identifier.class).stream()
        .map(Identifier::getValue) // TODO: preserve quotedness
        .collect(Collectors.toList());
    if (context.ASTERISK() != null) {
      parts.add("*");
    }

    return QualifiedName.of(parts);
  }
  private Optional<QualifiedName> getQualifiedNameIfPresent(
      SqlBaseParser.QualifiedNameContext context) {
    return Optional.ofNullable(context)
        .map(this::getQualifiedName);
  }

  private String getType(SqlBaseParser.TypeContext type) {
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

    if (type.ARRAY() != null) {
      return "ARRAY(" + getType(type.type(0)) + ")";
    }

    if (type.MAP() != null) {
      return "MAP(" + getType(type.type(0)) + "," + getType(type.type(1)) + ")";
    }

    if (type.ROW() != null) {
      StringBuilder builder = new StringBuilder("(");
      for (int i = 0; i < type.identifier().size(); i++) {
        if (i != 0) {
          builder.append(",");
        }
        builder.append(visit(type.identifier(i)))
            .append(" ")
            .append(getType(type.type(i)));
      }
      builder.append(")");
      return "ROW" + builder;
    }

    if (type.INTERVAL() != null) {
      return "INTERVAL " + getIntervalFieldType((Token) type.from.getChild(0).getPayload()) +
          " TO " + getIntervalFieldType((Token) type.to.getChild(0).getPayload());
    }

    throw new IllegalArgumentException("Unsupported type specification: " + type.getText());
  }

  private String typeParameterToString(SqlBaseParser.TypeParameterContext typeParameter) {
    if (typeParameter.INTEGER_VALUE() != null) {
      return typeParameter.INTEGER_VALUE().toString();
    }
    if (typeParameter.type() != null) {
      return getType(typeParameter.type());
    }
    throw new IllegalArgumentException("Unsupported typeParameter: " + typeParameter.getText());
  }

  private boolean mixedAndOrOperatorParenthesisCheck(Expression expression,
      SqlBaseParser.BooleanExpressionContext node, LogicalBinaryExpression.Operator operator) {
    if (expression instanceof LogicalBinaryExpression) {
      if (((LogicalBinaryExpression) expression).getOperator().equals(operator)) {
        if (node.children.get(0) instanceof SqlBaseParser.ValueExpressionDefaultContext) {
          return !(((SqlBaseParser.PredicatedContext) node).valueExpression().getChild(0) instanceof
              SqlBaseParser.ParenthesizedExpressionContext);
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
