package ai.dataeng.sqml.parser.sqrl.analyzer;

import static ai.dataeng.sqml.parser.SqrlNodeUtil.selectAlias;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.sqrl.analyzer.ExpressionAnalyzer.JoinResult;
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
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryBody;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SetOperation;
import ai.dataeng.sqml.tree.SimpleGroupBy;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatementAnalyzer extends AstVisitor<Scope, Scope> {
  public final Analyzer analyzer;

  public static final AliasGenerator gen = new AliasGenerator();

  private List<JoinResult> additionalJoins = new ArrayList<>();
  private List<Column> columns = new ArrayList<>();

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

    // Expand select items
    Select unqualifiedSelect = expand(node.getSelect(), scope);

    // We're doing a lot of transformations so convert grouping conditions to ordinals.
    Optional<GroupBy> unqualifiedGroupBy = node.getGroupBy().map(group->groupByOrdinal(unqualifiedSelect, group));

    // Qualify other expressions
    Select select = rewriteSelect(unqualifiedSelect, scope);
    Optional<Expression> having = rewriteHaving(node.getHaving(), scope);

    if (scope.getContextTable().isPresent()) {
      Optional<Table> table = scope.getJoinScope(Name.SELF_IDENTIFIER);
      Select contextSelect = appendGroupKeys(select, table.get().getPrimaryKeys());

      GroupBy groupBy = null;
      Relation from = (Relation)sourceScope.getNode();
      for (JoinResult result : additionalJoins) {
        from = new Join(Optional.empty(), result.getType(), from, result.getRelation(), result.getCriteria());
      }

      if (scope.isExpression()) {
        Table contextTable = scope.getContextTable().get();

        if (isAggregating(contextSelect)) {
          //Generate or append group by, keep track of new grouping keys
          int startIndex =
              contextSelect.getSelectItems().size() - table.get().getPrimaryKeys().size();
          IntStream groupIndex = IntStream.range(startIndex, contextSelect.getSelectItems().size());
          groupBy = unqualifiedGroupBy
              .map(group -> {
                List<Expression> grouping = new ArrayList<>(
                    group.getGroupingElement().getExpressions());
                grouping.addAll(toGroupByExpression(groupIndex));
                return new GroupBy(new SimpleGroupBy(grouping));
              })
              .orElse(toGroupBy(toGroupByExpression(IntStream.range(startIndex, contextSelect.getSelectItems().size()))));

          QuerySpecification querySpecification = new QuerySpecification(
              node.getLocation(),
              contextSelect,
              from,
              Optional.empty(),
              Optional.of(groupBy),
              having,
              Optional.empty(),
              Optional.empty()
          );
          Query query = new Query(querySpecification, Optional.empty(), Optional.empty());

          //Join on contextQuery, using the generated fields and only project our fields
          Name lhs = gen.nextTableAliasName();
          Name rhs = gen.nextTableAliasName();
          TableNode tableNode = new TableNode(Optional.empty(), contextTable.getId().toNamePath(), Optional.of(lhs));
          Join join = new Join(Optional.empty(), Type.LEFT, tableNode,
              new AliasedRelation(new TableSubquery(query), new Identifier(Optional.empty(), rhs.toNamePath())),
              getCriteria(contextSelect, startIndex, contextSelect.getSelectItems().size(), contextTable, lhs, rhs));
        } else {
          //Non aggregating expression, we can just add the columns directly into the query
          //SELECT coalesce(discount, 0.0) FROM entries => SELECT _uuid, quantity, discount, coalesce(discount, 0.0) AS discount$1 FROM entries
          List<SelectItem> additionalColumns = contextTable.getFields().getElements().stream()
              .filter(e -> e instanceof Column)
              .map(e -> new SingleColumn(new Identifier(Optional.empty(),
                  Name.SELF_IDENTIFIER.toNamePath().concat(e.getId().toNamePath())),
                  Optional.of(new Identifier(Optional.empty(), e.getId().toNamePath()))))
              .collect(Collectors.toList());
          List<SelectItem> list = new ArrayList<>(select.getSelectItems());
          list.addAll(additionalColumns);
          QuerySpecification querySpecification = new QuerySpecification(
              node.getLocation(),
              new Select(select.isDistinct(), list),
              from,
              Optional.empty(),
              Optional.empty(),
              having,
              Optional.empty(),
              Optional.empty()
          );

          return createScope(querySpecification, scope);
        }
      } else {
        int startIndex =
            contextSelect.getSelectItems().size() - table.get().getPrimaryKeys().size();

        //Add grouping of context fields
        if (node.getGroupBy().isPresent()) {
          groupBy = unqualifiedGroupBy
              .map(group -> {
                List<Expression> grouping = new ArrayList<>(
                    group.getGroupingElement().getExpressions());
                grouping.addAll(toGroupByExpression(IntStream.range(startIndex, contextSelect.getSelectItems().size())));
                return new GroupBy(new SimpleGroupBy(grouping));
              })
              .orElse(toGroupBy(toGroupByExpression(IntStream.range(startIndex, contextSelect.getSelectItems().size()))));
        }

        QuerySpecification querySpecification = new QuerySpecification(
            node.getLocation(),
            contextSelect,
            from,
            Optional.empty(),
            Optional.ofNullable(groupBy),
            having,
            Optional.empty(),
            Optional.empty()
        );
        return createScope(querySpecification, scope);
      }

    } else {
      throw new RuntimeException("Base table wip");
    }

    return null;
  }

  private Optional<JoinCriteria> getCriteria(Select contextSelect, int startIndex, int endIndex,
      Table contextTable, Name lhs, Name rhs) {
    List<Expression> conditions = new ArrayList<>();
    for (int i = startIndex; i < endIndex; i++) {
      SingleColumn innerColumn = (SingleColumn)contextSelect.getSelectItems().get(i);
      Identifier rhsColumn = new Identifier(Optional.empty(), rhs.toNamePath().concat(innerColumn.getAlias().get().getNamePath().getFirst()));

      Column outerColumn = (Column)contextTable.getField(((Identifier)innerColumn.getExpression()).getNamePath().getFirst());
      Identifier lhsColumn = new Identifier(Optional.empty(), lhs.toNamePath().concat(outerColumn.getId()));
      conditions.add(new ComparisonExpression(Optional.empty(), Operator.EQUAL, lhsColumn, rhsColumn));
    }

    return Optional.of(new JoinOn(Optional.empty(), and(conditions)));
  }

  private Select appendGroupKeys(Select select, List<Column> keys) {
    List<SelectItem> items = new ArrayList<>(select.getSelectItems());
    items
        .addAll(selectAlias(keys, Name.SELF_IDENTIFIER, gen));
    return new Select(items);
  }

  private List<Expression> toGroupByExpression(IntStream range) {
    return range
        .mapToObj(i->new LongLiteral(Integer.toString(i)))
        .collect(Collectors.toList());
  }
  private GroupBy toGroupBy(List<Expression> expr) {
    return new GroupBy(new SimpleGroupBy(expr));
  }

  private Optional<Expression> rewriteHaving(Optional<Expression> having, Scope scope) {
    return having.map(h->rewriteExpression(h, scope));
  }

  private Select rewriteSelect(Select select, Scope scope) {
    List<SelectItem> items = select.getSelectItems()
        .stream()
        .map(s->(SingleColumn)s)
        .map(s->new SingleColumn(rewriteExpression(s.getExpression(), scope), s.getAlias()))
        .collect(Collectors.toList());

    return new Select(select.getLocation(), select.isDistinct(), items);
  }

  private List<Expression> getOrdinal(Select select, GroupBy group) {
    Set<Integer> grouping = new HashSet<>();
    for (Expression expression : group.getGroupingElement().getExpressions()) {
      int index = IntStream.range(0, select.getSelectItems().size())
          .filter(i -> ((SingleColumn)select.getSelectItems().get(i)).getAlias().get().equals(expression))
          .findFirst()
          .orElseThrow(()-> new RuntimeException("Cannot find grouping element " + expression));

      grouping.add(index);
    }

    return grouping.stream()
        .map(i->(Expression)new LongLiteral(i.toString()))
        .collect(Collectors.toList());
  }

  private GroupBy groupByOrdinal(Select select, GroupBy group) {
    return new GroupBy(new SimpleGroupBy(getOrdinal(select, group)));
  }

  /**
   * Expands STAR alias
   */
  private Select expand(Select select, Scope scope) {
    List<SelectItem> rewritten = new ArrayList<>();
    for (SelectItem item : select.getSelectItems()) {
      if (item instanceof AllColumns) {
        Optional<Name> starPrefix = ((AllColumns) item).getPrefix()
            .map(e->e.getFirst());

        List<Identifier> fields = scope.resolveFieldsWithPrefix(starPrefix);

        for (Identifier identifier : fields) {
          rewritten.add(new SingleColumn(identifier,Optional.empty()));
        }
      } else if (item instanceof SingleColumn) {
        SingleColumn column = (SingleColumn) item;

        // We want to preserve the names of the columns

        NamePath name;

        if (scope.isExpression()) {
          Table table = scope.getContextTable().get();
          /*
           * Expression scoped queries only create a single column
           */
          Column column1 = table.fieldFactory(scope.getExpressionName());
          this.columns.add(column1);
          name = column1.getId().toNamePath();
        } else {
          if (column.getAlias().isPresent()) {
            name = column.getAlias().get().getNamePath();
          } else if(column.getExpression() instanceof Identifier) {
            name = ((Identifier)column.getExpression()).getNamePath();
          } else {
            name = gen.nextAliasName().toNamePath();
          }
        }

        rewritten.add(new SingleColumn(column.getExpression(), new Identifier(Optional.empty(), name)));
      }
      else {
        throw new IllegalArgumentException(String.format("Unsupported SelectItem type: %s", item.getClass().getName()));
      }
    }
    return new Select(rewritten);
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
    TableBookkeeping result = walkRemaining(tableNode, b, 1);

    scope.getJoinScope().put(result.getAlias(), result.getCurrentTable());

    return createScope(result.getCurrent(), scope);
  }

  public List<Column> getColumns() {
    return columns;
  }

  @Value
  public static class TableBookkeeping {
    Relation current;
    Name alias;
    Table currentTable;
  }

  private static Name getTableAlias(TableNode tableNode, int i) {
    if (tableNode.getNamePath().getLength() == 1 && i == 0) {
      return tableNode.getAlias().orElse(tableNode.getNamePath().getFirst());
    }

    if (i == tableNode.getNamePath().getLength() - 1) {
      return tableNode.getAlias().orElse(gen.nextTableAliasName());
    }

    return gen.nextTableAliasName();
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
      Name firstRelAlias = getTableAlias(rhs, 1);
      TableNode relation = new TableNode(Optional.empty(),
          firstRel.getToTable().getId().toNamePath(),
          Optional.of(firstRelAlias));

      TableBookkeeping b = new TableBookkeeping(relation, joinAlias, firstRel.getToTable());
      TableBookkeeping result = walkRemaining(rhs, b, 2);

      JoinOn criteria = (JoinOn) getCriteria(firstRel, joinAlias, firstRelAlias).get();

      scope.getJoinScope().put(result.getAlias(), result.getCurrentTable());

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
      TableBookkeeping result = walkRemaining(rhs, b, 1);


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

  public static TableBookkeeping walkRemaining(TableNode node, TableBookkeeping b, int startAt) {
    for (int i = startAt; i < node.getNamePath().getLength(); i++) {
      Relationship rel = (Relationship)b.getCurrentTable().getField(node.getNamePath().get(i));
      Name alias = getTableAlias(node, i);
      Join join = new Join(Optional.empty(), Type.INNER, b.getCurrent(),
          getRelation(rel, alias), getCriteria(rel, b.getAlias(), alias));
      b = new TableBookkeeping(join, alias, rel.getToTable());
    }
    return b;
  }

  public static Relation getRelation(Relationship rel, Name nextAlias) {
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


    } else {
      List<Column> columns = rel.getTable().getPrimaryKeys();
      return getCriteria(columns, alias, nextAlias);
    }
  }

  private static Identifier toIdentifier(Column c, Name alias) {
    return new Identifier(Optional.empty(), alias.toNamePath().concat(c.getId().toNamePath()));
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

  private Scope createScope(Node node, Scope parentScope) {
    return new Scope(parentScope.getContextTable(), node, parentScope.getJoinScope(),
        parentScope.isExpression(), parentScope.getExpressionName());
  }

  private boolean isAggregating(Node rewritten) {
    AggregationVisitor aggregationVisitor = new AggregationVisitor();
    rewritten.accept(aggregationVisitor, null);
    return aggregationVisitor.hasAgg();
  }

  private Expression rewriteExpression(Expression expression, Scope scope) {
    ExpressionAnalyzer analyzer = new ExpressionAnalyzer();
    Expression expr = analyzer.analyze(expression, scope);

    this.additionalJoins.addAll(analyzer.joinResults);
    return expr;
  }

  public static Expression and(List<Expression> expressions) {
    if (expressions.size() == 0) {
      return null;
    } else if (expressions.size() == 1) {
      return expressions.get(0);
    } else if (expressions.size() == 2) {
      return new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
          expressions.get(0),
          expressions.get(1));
    }

    return new LogicalBinaryExpression(LogicalBinaryExpression.Operator.AND,
        expressions.get(0), and(expressions.subList(1, expressions.size())));
  }
}
