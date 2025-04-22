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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.config.CharLiteralStyle;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.CalciteFixes;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCollectionTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlRowTypeNameSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqrlColumnDefinition;
import org.apache.calcite.sql.SqrlCreateDefinition;
import org.apache.calcite.sql.SqrlDistinctQuery;
import org.apache.calcite.sql.SqrlExportDefinition;
import org.apache.calcite.sql.SqrlExpressionQuery;
import org.apache.calcite.sql.SqrlFromQuery;
import org.apache.calcite.sql.SqrlImportDefinition;
import org.apache.calcite.sql.SqrlJoinQuery;
import org.apache.calcite.sql.SqrlSqlQuery;
import org.apache.calcite.sql.SqrlTableFunctionDef;
import org.apache.calcite.sql.SqrlTableParamDef;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;
import org.apache.flink.sql.parser.type.ExtendedSqlCollectionTypeNameSpec;

import com.datasqrl.calcite.SqrlConformance;
import com.datasqrl.parse.SqlBaseParser.ArrayTypeContext;
import com.datasqrl.parse.SqlBaseParser.AssignmentPathContext;
import com.datasqrl.parse.SqlBaseParser.BackQuotedIdentifierContext;
import com.datasqrl.parse.SqlBaseParser.BaseTypeContext;
import com.datasqrl.parse.SqlBaseParser.ColumnDefinitionContext;
import com.datasqrl.parse.SqlBaseParser.CreateDefinitionContext;
import com.datasqrl.parse.SqlBaseParser.CreateStatementContext;
import com.datasqrl.parse.SqlBaseParser.DecimalLiteralContext;
import com.datasqrl.parse.SqlBaseParser.DistinctQueryContext;
import com.datasqrl.parse.SqlBaseParser.DoubleLiteralContext;
import com.datasqrl.parse.SqlBaseParser.ExportDefinitionContext;
import com.datasqrl.parse.SqlBaseParser.ExportStatementContext;
import com.datasqrl.parse.SqlBaseParser.ExpressionContext;
import com.datasqrl.parse.SqlBaseParser.ExpressionQueryContext;
import com.datasqrl.parse.SqlBaseParser.FromQueryContext;
import com.datasqrl.parse.SqlBaseParser.FunctionArgumentDefContext;
import com.datasqrl.parse.SqlBaseParser.HintContext;
import com.datasqrl.parse.SqlBaseParser.HintItemContext;
import com.datasqrl.parse.SqlBaseParser.ImportDefinitionContext;
import com.datasqrl.parse.SqlBaseParser.ImportStatementContext;
import com.datasqrl.parse.SqlBaseParser.IntegerLiteralContext;
import com.datasqrl.parse.SqlBaseParser.JoinQueryContext;
import com.datasqrl.parse.SqlBaseParser.KeyValueContext;
import com.datasqrl.parse.SqlBaseParser.QualifiedNameContext;
import com.datasqrl.parse.SqlBaseParser.QuotedIdentifierContext;
import com.datasqrl.parse.SqlBaseParser.RowFieldContext;
import com.datasqrl.parse.SqlBaseParser.RowTypeContext;
import com.datasqrl.parse.SqlBaseParser.ScriptContext;
import com.datasqrl.parse.SqlBaseParser.SelectAllContext;
import com.datasqrl.parse.SqlBaseParser.SelectItemContext;
import com.datasqrl.parse.SqlBaseParser.SelectSingleContext;
import com.datasqrl.parse.SqlBaseParser.SingleStatementContext;
import com.datasqrl.parse.SqlBaseParser.SqlQueryContext;
import com.datasqrl.parse.SqlBaseParser.TableFunctionDefContext;
import com.datasqrl.parse.SqlBaseParser.TypeContext;
import com.datasqrl.parse.SqlBaseParser.TypeParameterContext;
import com.datasqrl.parse.SqlBaseParser.UnquotedIdentifierContext;
import com.datasqrl.sql.parser.impl.SqrlSqlParserImpl;
import com.google.common.base.Preconditions;

import lombok.SneakyThrows;
import lombok.Value;

/**
 * Builds the abstract syntax tree for an SQRL script
 */
public class AstBuilder
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
    var startIndex = query.start.getStartIndex();
    var stopIndex = query.stop.getStopIndex();
    var interval = new Interval(startIndex, stopIndex);
    var queryString = query.start.getInputStream().getText(interval);
    return parse(offset, queryString);
  }

  private SqlNode parseExpression(ParserRuleContext expression) {
    var startIndex = expression.start.getStartIndex();
    var stopIndex = expression.stop.getStopIndex();
    var interval = new Interval(startIndex, stopIndex);
    var queryString = expression.start.getInputStream().getText(interval);

    var offsetPos = getLocation(expression);

    return parseExpression(offsetPos, queryString);
  }

  @SneakyThrows
  public SqlNode parseExpression(SqlParserPos offset, String expression) {
    return parseSql(expression, offset, SqlParser::parseExpression);
  }

  private SqlNode parseSql(String sql, SqlParserPos offset,
      SqlParserFunction parseFunction) throws Exception {
    var parser = SqlParser.create(sql, createParserConfig());

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

  private SqlParserPos adjustOrderedUnionOffset(SqlNode node) {
    if (node.getKind() == SqlKind.ORDER_BY && ((SqlOrderBy)node).getOperandList().get(0).getKind() == SqlKind.UNION) {
      return ((SqlOrderBy)node).getOperandList().get(0).getParserPosition();
    }
    return node.getParserPosition();
  }

  private SqlNode adjustPosition(SqlNode node, SqlParserPos offset) {
    return node.accept(new PositionAdjustingSqlShuttle(offset, adjustOrderedUnionOffset(node)));
  }

  private Exception adjustSqlParseException(SqlParseException e, SqlParserPos offset) {
    var pos = PositionAdjustingSqlShuttle.adjustSinglePosition(offset, e.getPos());
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
  public SqlNode visitCreateStatement(CreateStatementContext ctx) {
    return visit(ctx.createDefinition());
  }

  @Override
  public SqlNode visitCreateDefinition(CreateDefinitionContext ctx) {
    return new SqrlCreateDefinition(getLocation(ctx),
        getNamePath(ctx.qualifiedName()),
        visit(ctx.columnDefinition(), SqrlColumnDefinition.class));
  }

  @Override
  public SqlNode visitColumnDefinition(ColumnDefinitionContext ctx) {
    return new SqrlColumnDefinition(getLocation(ctx),
        (SqlIdentifier)ctx.identifier().accept(this),
        getType(ctx.type()));
  }

  @Override
  public SqlNode visitImportStatement(ImportStatementContext ctx) {
    return visit(ctx.importDefinition());
  }

  @Override
  public SqlNode visitImportDefinition(ImportDefinitionContext ctx) {
    Optional<SqlIdentifier> alias = Optional.ofNullable(
        ctx.alias == null ? null : (SqlIdentifier) visit(ctx.alias));

    var identifier = getNamePath(ctx.qualifiedName());
    var importDef = new SqrlImportDefinition(getLocation(ctx), identifier, alias);

    return importDef;
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
    var assign = visitAssign(ctx.assignmentPath());

    var prefix = "SELECT * ";
    var sql = format("%s%s", prefix, extractString(ctx.fromDeclaration()));
    var location = getLocation(ctx.fromDeclaration());
    var pos = new SqlParserPos(location.getLineNum(),
        location.getColumnNum() - prefix.length());

    var sqlNode = parse(pos, sql);
    return new SqrlFromQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        sqlNode);
  }

  @Override
  public SqlNode visitJoinQuery(JoinQueryContext ctx) {
    var assign = visitAssign(ctx.assignmentPath());

    var startIndex = ctx.joinDeclaration().start.getStartIndex();
    var stopIndex = ctx.joinDeclaration().stop.getStopIndex();
    var interval = new Interval(startIndex, stopIndex);
    var queryString = ctx.start.getInputStream().getText(interval);
    queryString = String.format("SELECT * FROM @ AS @ %s", queryString);

    var colOffset = ctx.joinDeclaration().getStart().getCharPositionInLine() + 20;
    var rowOffset = ctx.joinDeclaration().getStart().getLine();
    var pos = getLocation(ctx.joinDeclaration());

    var query = parse(pos, queryString);


    return new SqrlJoinQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        (SqlSelect) query);
  }

  @Override
  public SqlNode visitDistinctQuery(DistinctQueryContext ctx) {
    var assign = visitAssign(ctx.assignmentPath());

    var table = extractString(ctx.distinctQuerySpec().identifier());
    var stringJoiner = new StringJoiner(",");
    for (var i = 0; i < ctx.distinctQuerySpec().onExpr().selectItem().size(); i++) {
      var context = ctx.distinctQuerySpec().onExpr().selectItem(i);
      var value = extractString(context);
      stringJoiner.add(value);
    }

    var orderExpr = (ctx.distinctQuerySpec().orderExpr != null)
        ? Optional.of(extractString(ctx.distinctQuerySpec().orderExpr))
        : Optional.empty();

    var sql = format("SELECT /*+ DISTINCT_ON */ %s FROM %s %s",
        stringJoiner,
        table,
        orderExpr.map(o->"ORDER BY " + o).orElse("")
    );

    var select = (SqlSelect)parse(new SqlParserPos(1, 1), sql);

    //Update the parsing position
    var selectList = select.getSelectList();
    SqlNodeList orderList = select.getOrderList();
    SqlNode tableItem = select.getFrom();


    var tableStart = tableItem.getParserPosition();
    var tableOffset = getLocation(ctx.distinctQuerySpec().identifier());
    tableItem = tableItem.accept(new PositionAdjustingSqlShuttle(tableOffset, tableStart));
    select.setFrom(tableItem);

    var selectOffset = getLocation(ctx.distinctQuerySpec().onExpr().selectItem(0));
    var selectStart = selectList.getParserPosition();
    selectList = (SqlNodeList) selectList.accept(new PositionAdjustingSqlShuttle(selectOffset, selectStart));
    select.setSelectList(selectList);

    if (ctx.distinctQuerySpec().orderExpr != null) {
      var orderOffset = getLocation(ctx.distinctQuerySpec().ORDER());
      var orderStart = orderList.getParserPosition();
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
    var startIndex = context.start.getStartIndex();
    var stopIndex = context.stop.getStopIndex();
    var interval = new Interval(startIndex, stopIndex);
    var queryString = context.start.getInputStream().getText(interval);
    return queryString;
  }

  @Override
  public SqlNode visitSqlQuery(SqlQueryContext ctx) {
    var assign = visitAssign(ctx.assignmentPath());

    var startOffset = getLocation(ctx.query());
    var query = parseQuery(ctx.query(), startOffset);

    return new SqrlSqlQuery(
        getLocation(ctx),
        assign.getHints(),
        assign.getIdentifier(),
        assign.getTableArgs(),
        query);
  }

  @Override
  public SqlNode visitExpressionQuery(ExpressionQueryContext ctx) {
    var assign = visitAssign(ctx.assignmentPath());

    var expression = parseExpression(ctx.expression());

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
    for (var i = 0; i < ctx.functionArgumentDef().size(); i++) {
      var fCtx = ctx.functionArgumentDef(i);
      var param = new SqrlTableParamDef(
          getLocation(fCtx),
          (SqlIdentifier)visit(fCtx.identifier()),
          getType(fCtx.type()),
          fCtx.expression() == null ? Optional.empty() :
              Optional.of(parseExpression(fCtx.expression())),
          i);
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
    var token = context.getText();
    var identifier = token.substring(1, token.length() - 1)
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
    var id = flatten(ids);
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

  private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
    return contexts.stream()
        .map(this::visit)
        .map(clazz::cast)
        .collect(toList());
  }

  private SqlDataTypeSpec getType(TypeContext ctx) {
    var nullable = ctx.NOT() == null;
    SqlTypeNameSpec typeNameSpec;
    if (ctx.rowType() != null) {
      typeNameSpec = getRowType(ctx.rowType(), getLocation(ctx));
    } else if (ctx.arrayType() != null) {
      typeNameSpec = getArrayType(ctx.arrayType(), getLocation(ctx));
    } else {
      typeNameSpec = getTypeName(ctx, ctx.baseType());
    }
    var sqlDataTypeSpec = new SqlDataTypeSpec(typeNameSpec, null, nullable,
        SqlParserPos.ZERO);
    return sqlDataTypeSpec;
  }

  private SqlTypeNameSpec getRowType(RowTypeContext ctx, SqlParserPos pos) {
    List<SqlIdentifier> fieldNames = new ArrayList<>();
    List<SqlDataTypeSpec> fieldTypes = new ArrayList<>();
    for (RowFieldContext fieldCtx : ctx.rowFieldList().rowField()) {
      fieldNames.add(new SqlIdentifier(fieldCtx.identifier().getText(), pos));

      var type = getType(fieldCtx.type());
      fieldTypes.add(type);
    }
    return new SqlRowTypeNameSpec(pos, fieldNames, fieldTypes);
  }

  private SqlTypeNameSpec getArrayType(ArrayTypeContext ctx, SqlParserPos pos) {
    var elementType = getType(ctx.type());
    SqlCollectionTypeNameSpec typeNameSpec = new ExtendedSqlCollectionTypeNameSpec(
        elementType.getTypeNameSpec(), elementType.getNullable(), SqlTypeName.ARRAY, true, pos);
    return typeNameSpec;
  }

  private SqlTypeNameSpec getTypeName(TypeContext ctx, BaseTypeContext baseType) {
    var id = (SqlIdentifier) visit(baseType.identifier());
    var typeName = Util.last(id.names);
    if (typeName.equalsIgnoreCase("int")) {
      typeName = "INTEGER"; // Already exists in Calcite for standard type conversion
    }

    // Check for ROW or ARRAY in the typeName to directly handle these cases
    if (typeName.equalsIgnoreCase("ROW") || typeName.equalsIgnoreCase("ARRAY")) {
      // These types are handled separately with their specific methods
      throw new UnsupportedOperationException("ROW and ARRAY types should be handled with their specific contexts");
    }

    SqlTypeName sqlTypeName = SqlTypeName.get(typeName.toUpperCase(Locale.ROOT));
    if (sqlTypeName == null) {
      return new SqlUserDefinedTypeNameSpec(typeName, getLocation(baseType));
    }
    if (!ctx.typeParameter().isEmpty()) {
      //todo: we assume precision, need to fill out the rest
      List<SqlNode> params = visit(ctx.typeParameter(), SqlNode.class);
      var literal = (SqlLiteral) params.get(0);
      return new SqlBasicTypeNameSpec(sqlTypeName,
          ((BigDecimal)literal.getValue()).intValue(), getLocation(baseType));
    }

    return new SqlBasicTypeNameSpec(sqlTypeName, getLocation(baseType));
  }

  @Override
  public SqlNode visitTypeParameter(TypeParameterContext ctx) {
    if (ctx.INTEGER_VALUE() != null) {
      return SqlLiteral.createExactNumeric(ctx.getText(), getLocation(ctx));
    }

    return ctx.type().accept(this);
  }

  @Override
  public SqlNode visit(ParseTree tree) {
    if (tree == null) {
      return null;
    }
    return super.visit(tree);
  }
}
