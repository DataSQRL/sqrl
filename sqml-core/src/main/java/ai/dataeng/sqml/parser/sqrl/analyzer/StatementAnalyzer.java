package ai.dataeng.sqml.parser.sqrl.analyzer;

import static ai.dataeng.sqml.parser.sqrl.AliasUtil.getTableAlias;
import static ai.dataeng.sqml.util.SqrlNodeUtil.mapToOrdinal;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.sqrl.JoinWalker;
import ai.dataeng.sqml.parser.sqrl.JoinWalker.WalkResult;
import ai.dataeng.sqml.parser.sqrl.analyzer.ExpressionAnalyzer.JoinResult;
import ai.dataeng.sqml.parser.sqrl.analyzer.aggs.AggregationDetector;
import ai.dataeng.sqml.parser.sqrl.function.FunctionLookup;
import ai.dataeng.sqml.parser.sqrl.transformers.Transformers;
import ai.dataeng.sqml.tree.AllColumns;
import ai.dataeng.sqml.tree.AstVisitor;
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
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.TableNode;
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

  public static final AliasGenerator gen = new AliasGenerator();

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

    Optional<OrderBy> orderBy = node.getOrderBy().map(order -> rewriteOrderBy(order, scope));

    // Qualify other expressions
    Select select = (Select)expandedSelect.accept(this, scope).getNode();
    Optional<Expression> where = node.getWhere().map(w->rewriteExpression(w, scope));
    Optional<Expression> having = node.getHaving().map(h->rewriteExpression(h, scope)); //todo identify and replace having clause
    Relation from = appendAdditionalJoins((Relation)sourceScope.getNode());
    QuerySpecification spec = new QuerySpecification(
        node.getLocation(),
        select,
        from,
        where,
        groupBy,
        having,
        orderBy,
        node.getLimit()
    );

    return createScope(rewriteQuerySpec(spec, scope), scope);
  }

  private OrderBy rewriteOrderBy(OrderBy order, Scope scope) {
    List<SortItem> items = order.getSortItems().stream()
        .map(s->new SortItem(s.getLocation(), rewriteExpression(s.getSortKey(), scope), s.getOrdering()))
        .collect(Collectors.toList());
    return new OrderBy(order.getLocation(), items);
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

    // Get schema table or context table as the first Table in table path
    Table table = (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) ?
        scope.getContextTable().get() :
        analyzer.getDag().getSchema().getByName(namePath.getFirst()).get();

    //Special case: Self identifier is added to join scope
    if (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) {
      scope.getJoinScope().put(Name.SELF_IDENTIFIER, scope.getContextTable().get());
    }

    Name firstAlias = namePath.getFirst().equals(Name.SELF_IDENTIFIER)
        ? Name.SELF_IDENTIFIER
        : getTableAlias(tableNode, 0, gen::nextTableAliasName);
    Name lastAlias = tableNode.getAlias().isPresent()
        ? tableNode.getAlias().get()
        : namePath.getLength() > 1 ? gen.nextTableAliasName() : namePath.getFirst();

    scope.getJoinScope().put(firstAlias, table);
    WalkResult result = new JoinWalker().walk(firstAlias, Optional.of(lastAlias), namePath.popFirst(), Optional.empty(),
        ()->Optional.empty(),
        scope.getJoinScope());

    scope.getFieldScope().put(lastAlias, table.walk(namePath.popFirst()).get());
    return createScope(result.getRelation(), scope);
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
    Name lastAlias = rhs.getAlias().isPresent()
        ? rhs.getAlias().get()
        : rhs.getNamePath().getLength() > 1
            ? Name.system(rhs.getNamePath().toString())  //todo needs to be unique
            : rhs.getNamePath().getFirst();

    //A join traversal, e.g. FROM orders AS o JOIN o.entries
    if (scope.getJoinScope().containsKey(rhs.getNamePath().getFirst())) {
      Table baseTable = scope.getJoinScope().get(rhs.getNamePath().getFirst());
      //Immediately register the field scope so the criteria can be resolved
      scope.getFieldScope().put(lastAlias, baseTable.walk(rhs.getNamePath().popFirst()).get());
      JoinWalker joinWalker = new JoinWalker();
      WalkResult result = joinWalker.walk(
          rhs.getNamePath().getFirst(),
          Optional.of(lastAlias),
          rhs.getNamePath().popFirst(),
          Optional.of((Relation)left.getNode()),
          ()->rewrite(node.getCriteria(), scope),
          scope.getJoinScope()
      );

      return createScope(result.getRelation(), scope);
    } else { //A regular join: FROM Orders JOIN entries
      Table table = analyzer.getDag().getSchema().getByName(rhs.getNamePath().get(0)).get();
      Name firstAlias = getTableAlias(rhs, 0, gen::nextTableAliasName);
      scope.getJoinScope().put(firstAlias, table);
      scope.getFieldScope().put(lastAlias, table.walk(rhs.getNamePath().popFirst()).get());

      TableNode tableNode = new TableNode(Optional.empty(), table.getId().toNamePath(), Optional.of(firstAlias));
      //If there is one entry in the path, do an inner join w/ the given criteria
      Type type = rhs.getNamePath().getLength() > 1 || node.getCriteria().isEmpty()
          ? Type.CROSS
          : Type.INNER;
      //Similar, we want to push in the first join criteria if possible
      Optional<JoinCriteria> firstCriteria = rhs.getNamePath().getLength() > 1
          ? Optional.empty()
          : node.getCriteria();

      Join join = new Join(Optional.empty(),
          type,
          (Relation)left.getNode(),
          tableNode,
          rewrite(firstCriteria, scope)
      );

      JoinWalker joinWalker = new JoinWalker();
      WalkResult result = joinWalker.walk(
          firstAlias,
          Optional.of(lastAlias),
          rhs.getNamePath().popFirst(),
          Optional.of(join),
          ()->rewrite(node.getCriteria(), scope),
          scope.getJoinScope()
      );

      return createScope(result.getRelation(), scope);
    }
  }

  private Optional<JoinCriteria> rewrite(Optional<JoinCriteria> criteria, Scope scope) {
    return criteria.map(e->
        new JoinOn(e.getLocation(), rewriteExpression(((JoinOn)e).getExpression(), scope)));
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

  private Scope createScope(Node node, Scope parentScope) {
    return new Scope(parentScope.getContextTable(), node, parentScope.getJoinScope(),
        parentScope.getFieldScope(),
        parentScope.isExpression(), parentScope.getExpressionName());
  }

  private Expression rewriteExpression(Expression expression, Scope scope) {
    ExpressionAnalyzer analyzer = new ExpressionAnalyzer();
    Expression expr = analyzer.analyze(expression, scope);

    this.additionalJoins.addAll(analyzer.joinResults);
    return expr;
  }
}
