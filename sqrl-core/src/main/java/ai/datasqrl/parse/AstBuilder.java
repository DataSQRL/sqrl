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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import ai.datasqrl.parse.SqlBaseParser.AssignContext;
import ai.datasqrl.parse.SqlBaseParser.CreateSubscriptionContext;
import ai.datasqrl.parse.SqlBaseParser.DistinctAssignmentContext;
import ai.datasqrl.parse.SqlBaseParser.ExportDefinitionContext;
import ai.datasqrl.parse.SqlBaseParser.ExportStatementContext;
import ai.datasqrl.parse.SqlBaseParser.ExpressionAssignContext;
import ai.datasqrl.parse.SqlBaseParser.HintContext;
import ai.datasqrl.parse.SqlBaseParser.ImportDefinitionContext;
import ai.datasqrl.parse.SqlBaseParser.ImportStatementContext;
import ai.datasqrl.parse.SqlBaseParser.JoinAssignmentContext;
import ai.datasqrl.parse.SqlBaseParser.QualifiedNameContext;
import ai.datasqrl.parse.SqlBaseParser.QueryAssignContext;
import ai.datasqrl.parse.SqlBaseParser.QuotedIdentifierContext;
import ai.datasqrl.parse.SqlBaseParser.ScriptContext;
import ai.datasqrl.parse.SqlBaseParser.UnquotedIdentifierContext;
import ai.datasqrl.parse.tree.CreateSubscription;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.ExportDefinition;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.Hint;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.SubscriptionType;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.SqrlConformance;
import ai.datasqrl.plan.calcite.hints.SqrlHintStrategyTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.logging.log4j.util.Strings;

/**
 * Builds the abstract syntax tree for an SQRL script using the classes in
 * {@link ai.datasqrl.parse.tree}.
 */
class AstBuilder
    extends SqlBaseBaseVisitor<Node> {

  AstBuilder(ParsingOptions parsingOptions) {
  }

  @SneakyThrows
  private SqlNode parseSql(String sql) {
    SqlNode node = SqlParser.create(sql,
        SqlParser.config().withCaseSensitive(false)
            .withConformance(SqrlConformance.INSTANCE)
            .withUnquotedCasing(Casing.UNCHANGED)
        ).parseQuery();
    return node;
  }

  private static String unquote(String value) {
    return value.substring(1, value.length() - 1)
        .replace("''", "'");
  }

  private static boolean isHexDigit(char c) {
    return ((c >= '0') && (c <= '9')) ||
        ((c >= 'A') && (c <= 'F')) ||
        ((c >= 'a') && (c <= 'f'));
  }

  private static boolean isValidUnicodeEscape(char c) {
    return c < 0x7F && c > 0x20 && !isHexDigit(c) && c != '"' && c != '+' && c != '\'';
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

  public List<Hint> getHints(HintContext hint) {
    if (hint == null) {
      return List.of();
    }
    return hint.hintItem().stream()
        .map(h -> (Hint) visitHintItem(h))
        .collect(toList());
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

    Optional<SingleColumn> timestamp;
    if (ctx.TIMESTAMP() != null) {

      Interval tableI = new Interval(
          ctx.expression().start.getStartIndex(),
          ctx.expression().stop.getStopIndex());
      String expr = ctx.expression().start.getInputStream().getText(tableI);

      if (ctx.timestampAlias != null) {
        Identifier timestampAlias = ((Identifier) visit(ctx.timestampAlias));
        timestamp = Optional.of(new SingleColumn(expr, Optional.of(timestampAlias)));
      } else {
        timestamp = Optional.of(new SingleColumn(expr));
      }
    } else {
      timestamp = Optional.empty();
    }

    return new ImportDefinition(getLocation(ctx), getNamePath(ctx.qualifiedName()), alias,
        timestamp);
  }

  @Override
  public Node visitExportStatement(ExportStatementContext ctx) {
    return visit(ctx.exportDefinition());
  }

  @Override
  public Node visitExportDefinition(ExportDefinitionContext ctx) {
    return new ExportDefinition(getLocation(ctx),
        getNamePath(ctx.qualifiedName(0)), getNamePath(ctx.qualifiedName(1)));
  }

  @Override
  public Node visitAssign(AssignContext context) {
    return context.assignment().accept(this);
  }

  @Override
  public Node visitDistinctAssignment(DistinctAssignmentContext ctx) {
    NamePath namePath = getNamePath(ctx.qualifiedName(0));

    Interval tableI = new Interval(
        ctx.table.start.getStartIndex(),
        ctx.table.stop.getStopIndex());
    String tableName = ctx.table.start.getInputStream().getText(tableI);

    Optional<String> alias = Optional.empty();
    if (ctx.identifier().size() > 1) {
      Interval aliasI = new Interval(
          ctx.identifier(1).start.getStartIndex(),
          ctx.identifier(1).stop.getStopIndex());
      alias = Optional.of(ctx.identifier(1).start.getInputStream().getText(aliasI));
    }

    List<String> pk = new ArrayList<>();
    for (int i = 1; i < ctx.qualifiedName().size(); i++) {
      Interval pkI = new Interval(
          ctx.qualifiedName(i).start.getStartIndex(),
          ctx.qualifiedName(i).stop.getStopIndex());
      String p = ctx.qualifiedName(i).start.getInputStream().getText(pkI);
      pk.add(p);
    }

    Interval sortI = new Interval(
        ctx.sortItem(0).start.getStartIndex(),
        ctx.sortItem(ctx.sortItem().size() - 1).stop.getStopIndex());
    String sort = ctx.sortItem(0).start.getInputStream().getText(sortI);

    String queryStr = createDistinctQuery(pk, sort, alias, tableName);
    SqlNode query = parseSql(queryStr);

    return new DistinctAssignment(
        Optional.of(getLocation(ctx)),
        namePath,
        tableName,
        alias,
        pk,
        sort,
        query,
        getHints(ctx.hint())
    );
  }

  private String createDistinctQuery(List<String> partitionKeys, String sort,
      Optional<String> alias, String table) {

    String pk = partitionKeys.stream().map(e -> "\"" + e + "\"")
        .collect(Collectors.joining(", "));
    String order = Strings.isEmpty(sort) ? "" : "ORDER BY " + sort;
    return String.format("SELECT /*+ %s(%s) */ * FROM %s %s %s LIMIT 1",
        SqrlHintStrategyTable.TOP_N, pk, table, alias.orElse(""), order);
  }

  @Override
  public Node visitJoinAssignment(JoinAssignmentContext ctx) {
    NamePath name = getNamePath(ctx.qualifiedName());
    Interval interval = new Interval(
        ctx.inlineJoin().start.getStartIndex(),
        ctx.inlineJoin().stop.getStopIndex());
    String query = ctx.inlineJoin().start.getInputStream().getText(interval);
    String queryStr = "SELECT * FROM _ " + query;
    SqlNode queryNode = mergeOuterOrderWithSelect(parseSql(queryStr));

    return new JoinAssignment(Optional.of(getLocation(ctx)), name,
        queryNode,
        getHints(ctx.hint()));
  }

  private SqlNode mergeOuterOrderWithSelect(SqlNode parseSql) {
    if (parseSql instanceof SqlOrderBy) {
      SqlOrderBy order = (SqlOrderBy) parseSql;
      if (order.query instanceof SqlSelect) {
        SqlSelect select = (SqlSelect) order.query;
        select.setOrderBy(order.orderList);
        select.setFetch(order.fetch);
        return select;
      }
    }
    return parseSql;
  }

  @Override
  public Node visitQueryAssign(QueryAssignContext ctx) {
    Interval interval = new Interval(
        ctx.query().start.getStartIndex(),
        ctx.query().stop.getStopIndex());
    String queryStr = ctx.query().start.getInputStream().getText(interval);
    SqlNode query = mergeOuterOrderWithSelect(parseSql(queryStr));
    return new QueryAssignment(Optional.of(getLocation(ctx)), getNamePath(ctx.qualifiedName()),
        query,
        getHints(ctx.hint()));
  }

  @Override
  public Node visitExpressionAssign(ExpressionAssignContext ctx) {
    NamePath name = getNamePath(ctx.qualifiedName());
    Interval interval = new Interval(
        ctx.expression().start.getStartIndex(),
        ctx.expression().stop.getStopIndex());
    String expression = ctx.expression().start.getInputStream().getText(interval);
    String queryStr = "SELECT " + expression + " FROM _";
    SqlNode query = parseSql(queryStr);
    return new ExpressionAssignment(Optional.of(getLocation(ctx)), name,
        query,
        getHints(ctx.hint()));
  }

  @Override
  public Node visitCreateSubscription(CreateSubscriptionContext ctx) {
    Interval interval = new Interval(
        ctx.query().start.getStartIndex(),
        ctx.query().stop.getStopIndex());
    String queryStr = ctx.query().start.getInputStream().getText(interval);
    SqlNode query = parseSql(queryStr);
    return new CreateSubscription(
        Optional.of(getLocation(ctx)),
        SubscriptionType.valueOf(ctx.subscriptionType().getText()),
        getNamePath(ctx.qualifiedName()),
        query
    );
  }

  @Override
  protected Node defaultResult() {
    return null;
  }

  @Override
  protected Node aggregateResult(Node aggregate, Node nextResult) {
    return null;
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


  private <T> List<T> visit(List<? extends ParserRuleContext> contexts, Class<T> clazz) {
    return contexts.stream()
        .map(this::visit)
        .map(clazz::cast)
        .collect(toList());
  }

  private NamePath getNamePath(QualifiedNameContext context) {
    List<Name> parts = visit(context.identifier(), Identifier.class).stream()
        .map(Identifier::getNamePath) // TODO: preserve quotedness
        .flatMap(e -> e.stream())
        .collect(Collectors.toList());
    if (context.all != null) {
      parts = new ArrayList<>(parts);
      parts.add(ReservedName.ALL);
    }

    return NamePath.of(parts);
  }

  @Override
  public Node visitQualifiedName(QualifiedNameContext ctx) {
    return new Identifier(getLocation(ctx), NamePath.parse(ctx.getText()));
  }


  private enum UnicodeDecodeState {
    EMPTY,
    ESCAPED,
    UNICODE_SEQUENCE
  }
}
