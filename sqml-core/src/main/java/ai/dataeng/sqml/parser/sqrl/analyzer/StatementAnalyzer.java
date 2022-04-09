package ai.dataeng.sqml.parser.sqrl.analyzer;

import static ai.dataeng.sqml.parser.sqrl.AliasUtil.getTableAlias;
import static ai.dataeng.sqml.parser.sqrl.AliasUtil.toIdentifier;
import static ai.dataeng.sqml.util.SqrlNodeUtil.and;
import static ai.dataeng.sqml.util.SqrlNodeUtil.mapToOrdinal;

import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.sqrl.analyzer.ExpressionAnalyzer.JoinResult;
import ai.dataeng.sqml.parser.sqrl.analyzer.aggs.AggregationDetector;
import ai.dataeng.sqml.parser.sqrl.function.FunctionLookup;
import ai.dataeng.sqml.parser.sqrl.transformers.Transformers;
import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.AllColumns;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.ComparisonExpression.Operator;
import ai.dataeng.sqml.tree.Except;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Intersect;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.Join.Type;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryBody;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SetOperation;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatementAnalyzer extends AstVisitor<Scope, Scope> {
  public final Analyzer analyzer;

  private final AggregationDetector aggregationDetector = new AggregationDetector(new FunctionLookup());

  private List<JoinResult> additionalJoins = new ArrayList<>();

  public StatementAnalyzer(Analyzer analyzer) {
    this.analyzer = analyzer;
  }

  @Override
  public Scope visitNode(Node node, Scope context) {
    throw new RuntimeException(String.format("Could not process node %s : %s", node.getClass().getName(), node));
  }

  @Override
  public Scope visitQuery(Query node, Scope scope) {
    Scope queryBodyScope = node.getQueryBody().accept(this, scope);
    Query query = new Query(node.getLocation(), (QueryBody) queryBodyScope.getNode(),
        node.getOrderBy(), node.getLimit());
    log.info("Generated Query: {}", NodeFormatter.accept(query));

    //TODO: order & limit for Set operations
    return createScope(query, scope);
  }

  @Override
  public Scope visitQuerySpecification(QuerySpecification node, Scope scope) {
    Scope sourceScope = node.getFrom().accept(this, scope);

    Select expandedSelect = expandStar(node.getSelect(), scope);

    // We're doing a lot of transformations so convert grouping conditions to ordinals.
    Optional<GroupBy> groupBy = node.getGroupBy().map(group -> mapToOrdinal(expandedSelect, group));
    Optional<OrderBy> orderBy = node.getOrderBy().map(order -> mapToOrdinal(expandedSelect, order));

    // Qualify other expressions
    Select select = (Select)expandedSelect.accept(this, scope).getNode();
    Optional<Expression> where = node.getWhere().map(w->rewriteExpression(w, scope));
    Optional<Expression> having = node.getHaving().map(h->rewriteExpression(h, scope));
    Relation from = appendAdditionalJoins((Relation)sourceScope.getNode());
    QuerySpecification spec = new QuerySpecification(
        node.getLocation(),
        select,
        from,
        where,
        groupBy,
        having,
        orderBy,
        Optional.empty()
    );

    return createScope(rewriteQuerySpec(spec, scope), scope);
  }

  /*
   * Rewrites queries to form compliant sql
   */
  private Node rewriteQuerySpec(QuerySpecification spec,
      Scope scope) {

    Optional<Table> contextTable = scope.getContextTable();

    QuerySpecification rewrittenNode = contextTable
        .map(t->{
          QuerySpecification rewritten = Transformers.addContextToQuery.transform(spec, t);
          return spec.getLimit().map(l -> Transformers.convertLimitToWindow.transform(rewritten, t))
              .orElse(rewritten);
        })
        .orElse(spec);

    if (scope.isExpression() && contextTable.isPresent()) {
      QuerySpecification query = Transformers.addColumnToQuery.transform(contextTable.get(), scope.getExpressionName(),
          aggregationDetector.isAggregating(spec.getSelect()), rewrittenNode);
      return query;
    }

    return rewrittenNode;
  }

  private Relation appendAdditionalJoins(Relation from) {
    for (JoinResult result : additionalJoins) {
      from = new Join(Optional.empty(), result.getType(), from, result.getRelation(), result.getCriteria());
    }
    return from;
  }

  /**
   * Note: This node is not walked from the rhs of a join.
   *
   * Expand the path, if applicable. The first name will always be either a SELF or a base table.
   * The remaining path are always relationships and are wrapped in a join. The final table
   * is aliased by either a given alias or generated alias.
   */
  @Override
  public Scope visitTable(TableNode tableNode, Scope scope) {
    NamePath namePath = tableNode.getNamePath();
    Table table = (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) ?
        scope.getContextTable().get() :
        analyzer.getDag().getSchema().getByName(namePath.getFirst()).get();

    //Special case: Self identifier is added to join scope
    if (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) {
      scope.getJoinScope().put(Name.SELF_IDENTIFIER, scope.getContextTable().get());
    }

    Name currentAlias;
    if (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) {
      currentAlias = Name.SELF_IDENTIFIER;
    } else {
      currentAlias = getTableAlias(tableNode, 0);
    }
    Relation relation = new TableNode(Optional.empty(), table.getId().toNamePath(), Optional.of(currentAlias));
    TableBookkeeping b = new TableBookkeeping(relation, currentAlias, table);
    TableBookkeeping result = walkJoin(tableNode, b, 1);

    scope.getJoinScope().put(result.getAlias(), result.getCurrentTable());

    return createScope(result.getCurrent(), scope);
  }

  /**
   * Joins have the assumption that the rhs will always be a TableNode so the rhs will be
   * unwrapped at this node, so it can provide additional criteria to the join.
   *
   * The rhs first table name will either be a join scoped alias or a base table. If it is
   * a join scoped alias, we will use that alias information to construct additional criteria
   * on the join, otherwise it'll be a cross join.
   */
  @Override
  public Scope visitJoin(Join node, Scope scope) {
    Scope left = node.getLeft().accept(this, scope);

    TableNode rhs = (TableNode)node.getRight();
    //A join traversal, e.g. FROM orders AS o JOIN o.entries
    if (scope.getJoinScope().containsKey(rhs.getNamePath().getFirst())) {
      Name joinAlias = rhs.getNamePath().getFirst();
      Table table = scope.getJoinScope(joinAlias).get();

      Relationship firstRel = (Relationship)table.getField(rhs.getNamePath().get(1));
      Name firstAlias = getTableAlias(rhs, 1);

      TableNode relation = new TableNode(Optional.empty(),
          firstRel.getToTable().getId().toNamePath(),
          Optional.of(firstAlias));

      TableBookkeeping b = new TableBookkeeping(relation, firstAlias, firstRel.getToTable());
      TableBookkeeping result = walkJoin(rhs, b, 2);

      scope.getJoinScope().put(result.getAlias(), result.getCurrentTable());

      JoinOn criteria = (JoinOn) getCriteria(firstRel, joinAlias, result.getAlias()).get();

      //Add criteria to join
      if (node.getCriteria().isPresent()) {
        List<Expression> newNodes = new ArrayList<>();
        newNodes.add(rewriteExpression(((JoinOn)node.getCriteria().get()).getExpression(), scope));
        newNodes.add(criteria.getExpression());
        criteria = new JoinOn(Optional.empty(), and(newNodes));
      }

      return createScope(
          new Join(node.getLocation(), node.getType(), (Relation) left.getNode(), (Relation) result.getCurrent(), Optional.of(criteria)),
          scope);
    } else { //A regular join: FROM Orders JOIN entries
      Table table = analyzer.getDag().getSchema().getByName(rhs.getNamePath().get(0)).get();
      Name joinAlias = getTableAlias(rhs, 0);
      TableNode tableNode = new TableNode(Optional.empty(), table.getId().toNamePath(), Optional.of(joinAlias));

      TableBookkeeping b = new TableBookkeeping(tableNode, joinAlias, table);
      //Remaining
      TableBookkeeping result = walkJoin(rhs, b, 1);

      scope.getJoinScope().put(result.getAlias(), result.getCurrentTable());

      Optional<JoinCriteria> criteria = node.getCriteria();
      if (node.getCriteria().isPresent()) {
        criteria = Optional.of(new JoinOn(Optional.empty(),
            rewriteExpression(((JoinOn)node.getCriteria().get()).getExpression(), scope)));
      }

      return createScope(
          new Join(node.getLocation(), node.getType(), (Relation) left.getNode(), (Relation) result.getCurrent(), criteria),
          scope);
    }
  }

  @Override
  public Scope visitUnion(Union node, Scope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public Scope visitIntersect(Intersect node, Scope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public Scope visitExcept(Except node, Scope scope) {
    return visitSetOperation(node, scope);
  }

  @Override
  public Scope visitSetOperation(SetOperation node, Scope scope) {
    return null;
  }

  @Override
  public Scope visitSelect(Select select, Scope scope) {
    List<SelectItem> items = select.getSelectItems().stream()
        .map(s->(SingleColumn)s)
        .map(s-> new SingleColumn(rewriteExpression(s.getExpression(), scope), s.getAlias()))
        .collect(Collectors.toList());

    return createScope(new Select(select.getLocation(), select.isDistinct(), items), scope);
  }

  /**
   * Expands STAR alias
   */
  private Select expandStar(Select select, Scope scope) {
    List<SelectItem> expanded = new ArrayList<>();
    for (SelectItem item : select.getSelectItems()) {
      if (item instanceof AllColumns) {
        Optional<Name> starPrefix = ((AllColumns) item).getPrefix()
            .map(e->e.getFirst());
        List<Identifier> fields = scope.resolveFieldsWithPrefix(starPrefix);
        for (Identifier identifier : fields) {
          expanded.add(new SingleColumn(identifier,Optional.empty()));
        }
      } else {
        expanded.add(item);
      }
    }
    return new Select(expanded);
  }

  public static TableBookkeeping walkJoin(TableNode node, TableBookkeeping b, int startAt) {
    for (int i = startAt; i < node.getNamePath().getLength(); i++) {
      Relationship rel = (Relationship)b.getCurrentTable().getField(node.getNamePath().get(i));
      Name alias = getTableAlias(node, i);
      Join join = new Join(Optional.empty(), Type.INNER, b.getCurrent(),
          expandRelation(rel, alias), getCriteria(rel, b.getAlias(), alias));
      b = new TableBookkeeping(join, alias, rel.getToTable());
    }
    return b;
  }

  public static Relation expandRelation(Relationship rel, Name nextAlias) {
    if (rel.getType() == Relationship.Type.JOIN) {
      return new AliasedRelation(Optional.empty(), new TableSubquery(Optional.empty(), (Query)rel.getNode()),
          new Identifier(Optional.empty(), nextAlias.toNamePath()));
    } else {
      return new TableNode(Optional.empty(), rel.getToTable().getId().toNamePath(),
          Optional.of(nextAlias));
    }
  }

  public static Optional<JoinCriteria> getCriteria(List<Column> columns, Name alias, Name nextAlias) {
    List<Expression> expr = columns.stream()
        .map(c -> new ComparisonExpression(Optional.empty(), Operator.EQUAL,
            toIdentifier(c, alias), toIdentifier(c, nextAlias)))
        .collect(Collectors.toList());
    return Optional.of(new JoinOn(Optional.empty(), and(expr)));
  }

  public static Optional<JoinCriteria> getCriteria(Relationship rel, Name alias, Name nextAlias) {
    if (rel.type == Relationship.Type.JOIN){
      //Join expansion uses the context table as its join
      //these can be renamed, just use pk1, pk2, etc for now
      List<Column> columns = rel.getTable().getPrimaryKeys();
      List<Expression> expr = new ArrayList<>();
      for (int i = 0; i < columns.size(); i++) {
        Column c = columns.get(i);
        ComparisonExpression comparisonExpression = new ComparisonExpression(Optional.empty(),
            Operator.EQUAL,
            toIdentifier(c, alias),
            new Identifier(Optional.empty(),
                nextAlias.toNamePath().concat(Name.system("_pk" + i).toNamePath()))
        );
        expr.add(comparisonExpression);
      }
      return Optional.of(new JoinOn(Optional.empty(), and(expr)));
    } else if (rel.type == Relationship.Type.PARENT) {
      List<Column> columns = rel.getToTable().getPrimaryKeys();
      return getCriteria(columns, alias, nextAlias);
    } else {
      List<Column> columns = rel.getTable().getPrimaryKeys();
      return getCriteria(columns, alias, nextAlias);
    }
  }

  private Scope createScope(Node node, Scope parentScope) {
    return new Scope(parentScope.getContextTable(), node, parentScope.getJoinScope(),
        parentScope.isExpression(), parentScope.getExpressionName());
  }

  private Expression rewriteExpression(Expression expression, Scope scope) {
    ExpressionAnalyzer analyzer = new ExpressionAnalyzer();
    Expression expr = analyzer.analyze(expression, scope);

    this.additionalJoins.addAll(analyzer.joinResults);
    return expr;
  }
}
