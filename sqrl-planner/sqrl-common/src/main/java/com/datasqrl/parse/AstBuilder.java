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

import com.datasqrl.calcite.SqrlConformance;
import com.datasqrl.model.StreamType;
import com.datasqrl.parse.SqlBaseParser.*;
import com.datasqrl.sql.parser.impl.SqrlSqlParserImpl;
import com.google.common.base.Preconditions;
import lombok.SneakyThrows;
import lombok.Value;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.util.*;
import org.apache.flink.table.planner.delegation.FlinkSqlParserFactories;

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

  public static SqlParser.Config createParserConfig() {
    return SqlParser.config()
        .withParserFactory(
            SqrlSqlParserImpl.FACTORY)
        .withConformance(SqrlConformance.INSTANCE)
        .withLex(Lex.JAVA)
        .withIdentifierMaxLength(256);

  }

  @SneakyThrows
  public SqlNode parse(String sql) {
    SqlParser parser = SqlParser.create(sql, createParserConfig());
    SqlNode node = parser.parseStmt();
    return CalciteFixes.pushDownOrder(node);
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

  @Value
  class AssignPathResult {
    Optional<SqlNodeList> hints;
    SqlIdentifier identifier;
    Optional<SqrlTableFunctionDef> tableArgs;
  }

  private AssignPathResult visitAssign(AssignmentPathContext ctx) {
    return new AssignPathResult(
        Optional.ofNullable((SqlNodeList) visit(ctx.hint())),
        (SqlIdentifier) visit(ctx.qualifiedName()),
        Optional.ofNullable((SqrlTableFunctionDef) visit(ctx.tableFunctionDef())));
  }

  @Override
  public SqlNode visitHint(HintContext ctx) {
    return new SqlNodeList(visit(ctx.hintItem(), SqlNode.class), getLocation(ctx));
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
    return new SqlParserPos(token.getLine(), Math.max(token.getCharPositionInLine() + 1, 1));
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

    SqlIdentifier identifier = getNamePath(ctx.qualifiedName());
    SqrlImportDefinition importDef = new SqrlImportDefinition(getLocation(ctx), identifier, alias);

    if (timestamp.isEmpty()) {
      return importDef;
    } else {
      SqrlAssignTimestamp assignTimestamp = new SqrlAssignTimestamp(getLocation(ctx),
          new SqlIdentifier(Util.last(identifier.names), identifier.getParserPosition()),
          alias,
          timestamp.get(), timestampAlias);

      return new SqlNodeList(List.of(importDef, assignTimestamp), importDef.getParserPosition());
    }
  }

  @Override
  public SqlNode visitExportStatement(ExportStatementContext ctx) {
    return visit(ctx.exportDefinition());
  }

  @Override
  public SqlNode visitExportDefinition(ExportDefinitionContext ctx) {
    return new SqrlExportDefinition(getLocation(ctx),
        getNamePath(ctx.qualifiedName(0)),
        getNamePath(ctx.qualifiedName(1)));
  }

  @Override
  public SqlNode visitFromQuery(FromQueryContext ctx) {
    AssignPathResult assign = visitAssign(ctx.assignmentPath());

    return new SqrlFromQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        (SqlSelect) visit(ctx.fromDeclaration()));
  }

  @Override
  public SqlNode visitJoinQuery(JoinQueryContext ctx) {
    AssignPathResult assign = visitAssign(ctx.assignmentPath());

    return new SqrlJoinQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        (SqlSelect) visit(ctx.joinDeclaration()));
  }

  @Override
  public SqlNode visitStreamQuery(StreamQueryContext ctx) {
    AssignPathResult assign = visitAssign(ctx.assignmentPath());
    return new SqrlStreamQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        visit(ctx.streamQuerySpec().query()),
        StreamType.valueOf(ctx.streamQuerySpec().subscriptionType().getText()));
  }

  @Override
  public SqlNode visitDistinctQuery(DistinctQueryContext ctx) {
    SqlIdentifier identifier = (SqlIdentifier)visit(ctx.distinctQuerySpec().identifier());
    AssignPathResult assign = visitAssign(ctx.assignmentPath());
    SqlNode table = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
        new SqrlCompoundIdentifier(getLocation(ctx),
        List.of(identifier)),identifier);

    List<SqlNode> expressions = new ArrayList<>();

    for (int i = 0; i < ctx.distinctQuerySpec().onExpr().selectItem().size(); i++) {
      SqlNode expr = visit(ctx.distinctQuerySpec().onExpr().selectItem(i));
      expressions.add(expr);
    }

    List<SqlNode> orders = new ArrayList<>();
    if (ctx.distinctQuerySpec().orderExpr != null) {
      SqlNode order = visit(ctx.distinctQuerySpec().orderExpr);
      orders.add(SqlStdOperatorTable.DESC.createCall(getLocation(ctx.distinctQuerySpec().orderExpr), order));
    }

    return new SqrlDistinctQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        table,
        expressions,
        orders);
  }

  @Override
  public SqlNode visitSqlQuery(SqlQueryContext ctx) {
    AssignPathResult assign = visitAssign(ctx.assignmentPath());

    int startIndex = ctx.query().start.getStartIndex();
    int stopIndex = ctx.query().stop.getStopIndex();
    Interval interval = new Interval(startIndex, stopIndex);
    String queryString = ctx.start.getInputStream().getText(interval);
    System.out.println(queryString);

    SqlNode query = parse(queryString);

    return new SqrlSqlQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        query);
  }

  @Override
  public SqlNode visitExpressionQuery(ExpressionQueryContext ctx) {
    AssignPathResult assign = visitAssign(ctx.assignmentPath());

    return new SqrlExpressionQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        visit(ctx.expression()));
  }

  @Override
  public SqlNode visitFromDeclaration(FromDeclarationContext ctx) {
    List<SqlNode> order = visit(ctx.sortItem(), SqlNode.class);
    Optional<SqlNodeList> sortList = emptyListToEmptyOptional(order);

    SqlNode fetch = null;
    Optional<String> limitStr = getTextIfPresent(ctx.limit);
    if (ctx.LIMIT() != null && limitStr.isPresent()) {
      fetch = SqlLiteral.createExactNumeric(limitStr.get(), getLocation(ctx.limit));
    }

    return new SqlSelect(getLocation(ctx),
        null,
        new SqlNodeList(List.of(SqlIdentifier.star(SqlParserPos.ZERO)), SqlParserPos.ZERO),
        visit(ctx.relation()),
        visitIfPresent(ctx.where, SqlNode.class).orElse(null),
        null,
        null,
        null,
        sortList.orElse(null),
        null,
        fetch,
        SqlNodeList.EMPTY);
  }

  @Override
  public SqlNode visitJoinDeclaration(JoinDeclarationContext ctx) {
    List<SqlNode> sort = visit(ctx.sortItem(), SqlNode.class);
    Optional<SqlNodeList> sortList = emptyListToEmptyOptional(sort);

    Optional<SqlNumericLiteral> limit =
        ctx.limit == null || ctx.limit.getText().equalsIgnoreCase("ALL")
            ? Optional.empty()
            : Optional.of(
            SqlLiteral.createExactNumeric(ctx.limit.getText(), getLocation(ctx.limit)));

    //we pull the first on condition to the where clause and turn it into a normal select
    SqlNode table = visit(ctx.first);
    SqlNode where = visit(ctx.firstCondition);

    for (int i = 1; i < ctx.remainingJoins().size(); i++) {
      SqlNode relation = visit(ctx.remainingJoins(i).aliasedRelation());
      SqlNode condition = visit(ctx.remainingJoins(i).joinCondition());
      table = new SqlJoin(getLocation(ctx.remainingJoins(i).joinCondition()),
          table,
          SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
          toJoinType(ctx.remainingJoins(i).joinType()).symbol(getLocation(ctx.remainingJoins(i).joinType())),
          relation,
          condition == null
              ? JoinConditionType.NONE.symbol(SqlParserPos.ZERO)
              : JoinConditionType.ON.symbol(getLocation(ctx.remainingJoins(i).joinCondition().ON())),
          condition);
    }

    return new SqlSelect(getLocation(ctx),
        null,
        new SqlNodeList(List.of(SqlIdentifier.STAR), SqlParserPos.ZERO),
        table,
        where,
        null,
        null,
        null,
        sortList.orElse(null),
        null,
        limit.orElse(null),
        SqlNodeList.EMPTY);
  }

  @Override
  public SqlNode visitJoinCondition(JoinConditionContext ctx) {
    return visit(ctx.booleanExpression());
  }

  @Override
  public SqlNode visitTableFunctionDef(TableFunctionDefContext ctx) {
    List<SqrlTableParamDef> defs = new ArrayList<>();
    for (int i = 0; i < ctx.functionArgumentDef().size(); i++) {
      FunctionArgumentDefContext fCtx = ctx.functionArgumentDef(i);
      SqrlTableParamDef param = new SqrlTableParamDef(
          getLocation(fCtx),
          (SqlIdentifier)visit(fCtx.identifier()),
          getType(fCtx.type()),
          Optional.ofNullable(visit(fCtx.literal())),
          i, false);
      defs.add(param);
    }

    return new SqrlTableFunctionDef(
        getLocation(ctx),
        defs);
  }

  @Override
  public SqlNode visitQuery(QueryContext context) {
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
    List<SqlNode> groups = visit(context.expression(), SqlNode.class);
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

    Optional<SqlNode> alias = visitIfPresent(context.identifier(), SqlNode.class);
    if (alias.isPresent()) {
      return SqlStdOperatorTable.AS.createCall(getLocation(context),
          expression,
          alias.get());
    }
    return expression;
  }

  @Override
  public SqlNode visitSubquery(SubqueryContext context) {
    return visit(context.query());
  }

  // ********************* primary expressions **********************

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
  }

  @Override
  public SqlNode visitRelationDefault(RelationDefaultContext ctx) {
    return visit(ctx.aliasedRelation());
  }

  @Override
  public SqlNode visitTableFunction(TableFunctionContext ctx) {
    SqlIdentifier node = (SqlIdentifier)visit(ctx.identifier());
    SqlUnresolvedFunction function = new SqlUnresolvedFunction(node, null,
        null, null, null, SqlFunctionCategory.USER_DEFINED_FUNCTION);
    SqlNodeList args = (SqlNodeList)visit(ctx.tableFunctionExpression());
    SqlNode call = function.createCall(args);
    return call;
  }

  @Override
  public SqlNode visitTableFunctionExpression(TableFunctionExpressionContext ctx) {
    return emptyListToEmptyOptional(visit(ctx.expression(), SqlNode.class))
        .orElse(SqlNodeList.EMPTY);
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

  public JoinType toJoinType(JoinTypeContext ctx) {
    JoinType joinType;

    if (ctx.LEFT() != null) {
      if (ctx.TEMPORAL() != null) {
        joinType = JoinType.LEFT_TEMPORAL;
      } else if (ctx.INTERVAL() != null) {
        joinType = JoinType.LEFT_INTERVAL;
      } else if (ctx.OUTER() != null){
        joinType = JoinType.LEFT;
      } else  {
        joinType = JoinType.LEFT_DEFAULT;
      }
    } else if (ctx.RIGHT() != null) {
      if (ctx.TEMPORAL() != null) {
        joinType = JoinType.RIGHT_TEMPORAL;
      } else if (ctx.INTERVAL() != null) {
        joinType = JoinType.RIGHT_INTERVAL;
      } else if (ctx.OUTER() != null){
        joinType = JoinType.RIGHT;
      } else  {
        joinType = JoinType.RIGHT_DEFAULT;
      }
    } else if (ctx.INNER() != null) {
      joinType = JoinType.INNER;
    } else if (ctx.TEMPORAL() != null) {
      joinType = JoinType.TEMPORAL;
    } else if (ctx.INTERVAL() != null) {
      joinType = JoinType.INTERVAL;
    } else {
      joinType = JoinType.DEFAULT;
    }

    return joinType;
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

    SqlOperator op = context.NOT() == null ? SqlStdOperatorTable.IS_NULL : SqlStdOperatorTable.IS_NOT_NULL;

    return op.createCall(getLocation(context), child);
  }

  @Override
  public SqlNode visitInList(InListContext context) {
    SqlNode result = SqlStdOperatorTable.IN.createCall(
        getLocation(context),
        (SqlNode) visit(context.value),
        new SqlNodeList(visit(context.expression(), SqlNode.class),
            getLocation(context)));

    if (context.NOT() != null) {
      result = SqlStdOperatorTable.NOT_IN.createCall(getLocation(context), result);
    }

    return result;
  }

  @Override
  public SqlNode visitInSubquery(InSubqueryContext context) {
    SqlNode query = visit(context.query());
    SqlOperator op = context.NOT() == null ? SqlStdOperatorTable.IN : SqlStdOperatorTable.NOT_IN;

    SqlNode result = op.createCall(
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
    return getNamePath(ctx);
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
    boolean distinct = isDistinct(context.setQuantifier());
    SqlIdentifier name = (SqlIdentifier) context.identifier().accept(this);
    List<SqlNode> args;
    if (context.expression() != null) {
      args = visit(context.expression(), SqlNode.class);
    } else if (context.query() != null){
      args = List.of(visit(context.query()));
    } else {
      throw new RuntimeException("Unknown function ast");
    }

    //special case: fnc(*)
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
    SqlCall call = function.createCall(getLocation(context), args);
    if (distinct) {
      final SqlLiteral qualifier =
          distinct ? SqlSelectKeyword.DISTINCT.symbol(getLocation(context.setQuantifier())) : null;

      return call.getOperator()
          .createCall(qualifier, call.getParserPosition(),
              call.getOperandList().toArray(SqlNode[]::new));
    } else {
      return call;
    }
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
  public SqlNode visitLiteralExpression(LiteralExpressionContext ctx) {
    return visit(ctx.literal());
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
    return SqlLiteral.createCharString(decodeUnicodeLiteral(context), getLocation(context));
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
      case WEEK:
      case YEAR:
      case MONTH:
      default:
        throw new RuntimeException(
            String.format("Found interval '%s'. YEAR MONTH WEEK interval not yet supported.",
                timeUnit));
      case DAY:
      case HOUR:
      case MINUTE:
      case SECOND:
        return SqlLiteral.createInterval(sign,
            expr.toValue(),
            new SqlIntervalQualifier(timeUnit, getPrecision(expr.toValue()),
                null, getFracPrecision(expr.toValue()), getLocation(context.intervalField())),
            getLocation(context)
        );
    }
  }

  public static int getFracPrecision(String toValue) {
    String[] val = toValue.split("\\.");
    if (val.length == 2) {
      return val[1].length();
    }

    return -1;
  }

  public static int getPrecision(String toValue) {
    return toValue.split("\\.")[0].length();
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
    List<SqlNode> nodes = visit(ctx.statement(), SqlNode.class);

    //post process: split aggregate statements
    List<SqlNode> newNodes = new ArrayList<>();
    for (SqlNode node : nodes) {
      if (node instanceof SqlNodeList) {
        newNodes.addAll(((SqlNodeList) node).getList());
      } else {
        newNodes.add(node);
      }
    }


    return new ScriptNode(
        getLocation(ctx),
        newNodes
    );
  }

  private SqlIdentifier getNamePath(QualifiedNameContext context) {
    List<SqlIdentifier> ids = visit(context.identifier(), SqlIdentifier.class);
    SqlIdentifier id = flatten(ids);
    Preconditions.checkState(
        id.names.size() == CalciteFixes.getComponentPositions(id).size());

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
      cPos.addAll(CalciteFixes.getComponentPositions(i));
    }

    return new SqlIdentifier(names, ids.get(0).getCollation(), ids.get(0).getParserPosition(),
        cPos);
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
    SqlIdentifier id = (SqlIdentifier) visit(baseType.identifier());

    String typeName = Util.last(id.names);
    if (typeName.equalsIgnoreCase("int")) {
      typeName = "INTEGER"; //exists in calcite parser conversion
    }

    SqlTypeName sqlTypeName = SqlTypeName.get(typeName.toUpperCase(Locale.ROOT));

    if (sqlTypeName == null) {
      return new SqlUserDefinedTypeNameSpec(typeName, getLocation(baseType));
    }
    return new SqlBasicTypeNameSpec(sqlTypeName, getLocation(baseType));
  }

  @Override
  public SqlNode visitQueryTermDefault(QueryTermDefaultContext ctx) {
    return visit(ctx.queryPrimary());
  }

  @Override
  public SqlNode visitQueryPrimary(QueryPrimaryContext ctx) {
    return Optional.ofNullable(visit(ctx.querySpecification()))
        .orElseGet(()->visit(ctx.subquery()));
  }

  @Override
  public SqlNode visitRelationPrimary(RelationPrimaryContext ctx) {
    return new SqrlCompoundIdentifier(getLocation(ctx), visit(ctx.relationItem(), SqlNode.class));
  }

  @Override
  public SqlNode visitRelationItem(RelationItemContext ctx) {
    if (ctx.subquery() != null) {
      return visit(ctx.subquery());
    } else if(ctx.identifier() != null) {
      return visit(ctx.identifier());
    } else if(ctx.tableFunction() != null) {
      return visit(ctx.tableFunction());
    }
    throw new RuntimeException("Unknown path");
  }

  @Override
  public SqlNode visitAliasedRelation(AliasedRelationContext ctx) {
    if (ctx.identifier() == null) {
      SqlNode node = visit(ctx.relationPrimary());
      //Automatically alias tables that are singular, e.g. FROM Orders
      if (node instanceof SqrlCompoundIdentifier && ((SqrlCompoundIdentifier)node).getItems().size() == 1
      && ((SqrlCompoundIdentifier)node).getItems().get(0) instanceof SqlIdentifier) {
        SqlNode identifer = ((SqrlCompoundIdentifier) node).getItems().get(0);
        return SqlStdOperatorTable.AS.createCall(
            getLocation(ctx),
            node,
            new SqlIdentifier(((SqlIdentifier)identifer).names.get(0),
                identifer.getParserPosition()));
      } else {
        return visit(ctx.relationPrimary());
      }

    }
    return SqlStdOperatorTable.AS.createCall(
        getLocation(ctx),
        visit(ctx.relationPrimary()),
        visit(ctx.identifier()));
  }

  private enum UnicodeDecodeState {
    EMPTY,
    ESCAPED,
    UNICODE_SEQUENCE
  }

  @Override
  public SqlNode visit(ParseTree tree) {
    if (tree == null) {
      return null;
    }
    return super.visit(tree);
  }
}
