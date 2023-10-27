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
import org.apache.calcite.config.CharLiteralStyle;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import java.util.*;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Builds the abstract syntax tree for an SQRL script
 */
class AstBuilder
    extends SqlBaseBaseVisitor<SqlNode> {

  public static SqlParser.Config createParserConfig() {
    return SqlParser.config()
        .withLex(Lex.JAVA)
        .withIdentifierMaxLength(256)
        .withParserFactory(
            SqrlSqlParserImpl.FACTORY)
        .withConformance(SqrlConformance.INSTANCE)
        .withCharLiteralStyles(Set.of(CharLiteralStyle.STANDARD));
  }

  @FunctionalInterface
  private interface SqlParserFunction {
    SqlNode apply(SqlParser parser) throws SqlParseException;
  }

  /**
   * Parses a SQL statement and adjusts its position.
   */
  @SneakyThrows
  public SqlNode parse(SqlParserPos offset, String sql) {
    return parseSql(sql, offset, SqlParser::parseStmt);
  }

  private SqlNode parseQuery(ParserRuleContext query, SqlParserPos offset) {
    int startIndex = query.start.getStartIndex();
    int stopIndex = query.stop.getStopIndex();
    Interval interval = new Interval(startIndex, stopIndex);
    String queryString = query.start.getInputStream().getText(interval);
    return parse(offset, queryString);
  }

  private SqlNode parseExpression(ParserRuleContext expression) {
    int startIndex = expression.start.getStartIndex();
    int stopIndex = expression.stop.getStopIndex();
    Interval interval = new Interval(startIndex, stopIndex);
    String queryString = expression.start.getInputStream().getText(interval);

    SqlParserPos offsetPos = getLocation(expression);

    return parseExpression(offsetPos, queryString);
  }

  @SneakyThrows
  public SqlNode parseExpression(SqlParserPos offset, String expression) {
    return parseSql(expression, offset, SqlParser::parseExpression);
  }

  private SqlNode parseSql(String sql, SqlParserPos offset,
      SqlParserFunction parseFunction) throws Exception {
    SqlParser parser = SqlParser.create(sql, createParserConfig());

    SqlNode node;

    try {
      node = parseFunction.apply(parser);
      node = CalciteFixes.pushDownOrder(node);
    } catch (SqlParseException e) {
      throw adjustSqlParseException(e, offset);
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    return adjustPosition(node, offset);
  }

  private SqlNode adjustPosition(SqlNode node, SqlParserPos offset) {
    return node.accept(new PositionAdjustingSqlShuttle(offset, node.getParserPosition()));
  }

  private Exception adjustSqlParseException(SqlParseException e, SqlParserPos offset) {
    SqlParserPos pos = PositionAdjustingSqlShuttle.adjustSinglePosition(offset, e.getPos());
    return new SqlParseException(e.getMessage(), pos, e.getExpectedTokenSequences(),
        e.getTokenImages(), e.getCause());
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
      SqlNode expr = parseExpression(ctx.expression());

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

    String prefix = "SELECT * ";
    String sql = format("%s%s", prefix, extractString(ctx.fromDeclaration()));
    SqlParserPos location = getLocation(ctx.fromDeclaration());
    SqlParserPos pos = new SqlParserPos(location.getLineNum(),
        location.getColumnNum() - prefix.length());

    SqlNode sqlNode = parse(pos, sql);
    return new SqrlFromQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        sqlNode);
  }

  @Override
  public SqlNode visitJoinQuery(JoinQueryContext ctx) {
    AssignPathResult assign = visitAssign(ctx.assignmentPath());

    int startIndex = ctx.joinDeclaration().start.getStartIndex();
    int stopIndex = ctx.joinDeclaration().stop.getStopIndex();
    Interval interval = new Interval(startIndex, stopIndex);
    String queryString = ctx.start.getInputStream().getText(interval);
    queryString = String.format("SELECT * FROM @ AS @ %s", queryString);

    int colOffset = ctx.joinDeclaration().getStart().getCharPositionInLine() + 20;
    int rowOffset = ctx.joinDeclaration().getStart().getLine();
    SqlParserPos pos = getLocation(ctx.joinDeclaration());

    SqlNode query = parse(pos, queryString);


    return new SqrlJoinQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        (SqlSelect) query);
  }

  @Override
  public SqlNode visitStreamQuery(StreamQueryContext ctx) {
    AssignPathResult assign = visitAssign(ctx.assignmentPath());
    SqlParserPos offset = getLocation(ctx.streamQuerySpec().query());
    SqlNode query = parseQuery(ctx.streamQuerySpec().query(), offset);

    return new SqrlStreamQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        query,
        StreamType.valueOf(ctx.streamQuerySpec().subscriptionType().getText()));
  }

  @Override
  public SqlNode visitDistinctQuery(DistinctQueryContext ctx) {
    AssignPathResult assign = visitAssign(ctx.assignmentPath());

    String table = extractString(ctx.distinctQuerySpec().identifier());
    StringJoiner stringJoiner = new StringJoiner(",");
    for (int i = 0; i < ctx.distinctQuerySpec().onExpr().selectItem().size(); i++) {
      SelectItemContext context = ctx.distinctQuerySpec().onExpr().selectItem(i);
      String value = extractString(context);
      stringJoiner.add(value);
    }

    Optional<String> orderExpr = (ctx.distinctQuerySpec().orderExpr != null)
        ? Optional.of(extractString(ctx.distinctQuerySpec().orderExpr))
        : Optional.empty();

    String sql = format("SELECT /*+ DISTINCT_ON */ %s FROM %s %s",
        stringJoiner,
        table,
        orderExpr.map(o->"ORDER BY " + o).orElse("")
    );

    SqlSelect select = (SqlSelect)parse(new SqlParserPos(1, 1), sql);

    //Update the parsing position
    SqlNodeList selectList = select.getSelectList();
    SqlNodeList orderList = select.getOrderList();
    SqlNode tableItem = select.getFrom();


    SqlParserPos tableStart = tableItem.getParserPosition();
    SqlParserPos tableOffset = getLocation(ctx.distinctQuerySpec().identifier());
    tableItem = tableItem.accept(new PositionAdjustingSqlShuttle(tableOffset, tableStart));
    select.setFrom(tableItem);

    SqlParserPos selectOffset = getLocation(ctx.distinctQuerySpec().onExpr().selectItem(0));
    SqlParserPos selectStart = selectList.getParserPosition();
    selectList = (SqlNodeList) selectList.accept(new PositionAdjustingSqlShuttle(selectOffset, selectStart));
    select.setSelectList(selectList);

    if (ctx.distinctQuerySpec().orderExpr != null) {
      SqlParserPos orderOffset = getLocation(ctx.distinctQuerySpec().ORDER());
      SqlParserPos orderStart = orderList.getParserPosition();
      orderList = (SqlNodeList) orderList.accept(new PositionAdjustingSqlShuttle(orderOffset, orderStart));
      select.setOrderBy(orderList);
    }

    return new SqrlDistinctQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        select);
  }

  private String extractString(ParserRuleContext context) {
    int startIndex = context.start.getStartIndex();
    int stopIndex = context.stop.getStopIndex();
    Interval interval = new Interval(startIndex, stopIndex);
    String queryString = context.start.getInputStream().getText(interval);
    return queryString;
  }

  @Override
  public SqlNode visitSqlQuery(SqlQueryContext ctx) {
    AssignPathResult assign = visitAssign(ctx.assignmentPath());

    SqlParserPos startOffset = getLocation(ctx.query());
    SqlNode query = parseQuery(ctx.query(), startOffset);

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

    SqlNode expression = parseExpression(ctx.expression());

    return new SqrlExpressionQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        expression);
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
          fCtx.expression() == null ? Optional.empty() :
              Optional.of(parseExpression(fCtx.expression())),
          i, false);
      defs.add(param);
    }

    return new SqrlTableFunctionDef(
        getLocation(ctx),
        defs);
  }

  @Override
  public SqlNode visitSelectAll(SelectAllContext context) {
    return SqlIdentifier.star(getLocation(context.ASTERISK()));
  }

  @Override
  public SqlNode visitSelectSingle(SelectSingleContext context) {
    return visit(context.expression());
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
    return parseExpression(ctx);
  }

  @Override
  public SqlNode visitQualifiedName(QualifiedNameContext ctx) {
    return getNamePath(ctx);
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
  public SqlNode visit(ParseTree tree) {
    if (tree == null) {
      return null;
    }
    return super.visit(tree);
  }
}
