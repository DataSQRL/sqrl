package ai.dataeng.sqml.parser.sqrl.analyzer;

import static ai.dataeng.sqml.parser.SqrlNodeUtil.ident;
import static ai.dataeng.sqml.parser.SqrlNodeUtil.selectAlias;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.FieldPath;
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
import ai.dataeng.sqml.tree.GroupingElement;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Intersect;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.Join.Type;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryBody;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SetOperation;
import ai.dataeng.sqml.tree.SimpleGroupBy;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap.Builder;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.util.Pair;

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

    //TODO: order & limit for Set operations
    return createScope(new Query(node.getLocation(), (QueryBody) queryBodyScope.getNode(),
        node.getOrderBy(), node.getLimit()), scope);
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

    // If we're in a nested context, append context keys
    if (scope.getContextTable().isPresent()) {
      Optional<Table> table = scope.getJoinScope(Name.SELF_IDENTIFIER);
      Select contextSelect = appendGroupKeys(select, table.get().getPrimaryKeys());

      GroupBy groupBy = null;
      if (isAggregating(contextSelect)) {
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
            .orElse(toGroupBy(toGroupByExpression(groupIndex)));
      }

      Relation from = (Relation)sourceScope.getNode();
      for (JoinResult result : additionalJoins) {
        from = new Join(Optional.empty(), result.getType(), from, result.getSubquery(), result.getCriteria());
      }

      if (scope.isExpression()) {
        //rejoin column back to table
        //If this is an aggregating expression then join, other add column
        Table contextTable = scope.getContextTable().get();

        List<SelectItem> additionalColumns = contextTable.getFields().getElements().stream()
            .filter(e-> e instanceof Column)
            .map(e->new SingleColumn(new Identifier(Optional.empty(), e.getId().toNamePath())))
            .collect(Collectors.toList());
        List<SelectItem> list = new ArrayList<>(select.getSelectItems());
        list.addAll(additionalColumns);

        QuerySpecification querySpecification = new QuerySpecification(
            node.getLocation(),
            new Select(select.isDistinct(), list),
            from,
            Optional.empty(),
            Optional.ofNullable(groupBy),
            having,
            Optional.empty(),
            Optional.empty()
        );

        return createScope(querySpecification, scope);
      } else {
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

    }




//    Pair<List<SelectItem>, List<Expression>> outputExpressions = analyzeSelect(node, sourceScope);
//    List<Expression> groupByExpressions = analyzeGroupBy(node, sourceScope.getNode(), sourceScope, outputExpressions);
//    List<Expression> groupByExpressions = analyzeGroupBy(node, sourceScope, outputExpressions);
//    analyzeHaving(node, sourceScope);

    //analyzeOrderBy(node.getOrderBy(), sourceScope);
//
//    //todo: check if there is a context query
//    Optional<GroupBy> groupBy = groupByExpressions != null && groupByExpressions.size() > 0 ?
//      Optional.of(new GroupBy(Optional.empty(), new SimpleGroupBy(Optional.empty(), groupByExpressions)))
//      :Optional.empty();


    return null;

//    return createScope(querySpecification, scope);
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
          .filter(i -> ((SingleColumn)select.getSelectItems().get(i)).getExpression().equals(expression))
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

        List<Field> fields = scope.resolveFieldsWithPrefix(starPrefix);

        for (Field field : fields) {
          Identifier identifier = new Identifier(item.getLocation(), field.getName().toNamePath());
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

    Name currentAlias = getTableAlias(tableNode, 0);
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
    if (scope.getJoinScope().containsKey(rhs.getNamePath().getFirst())) {
      Name joinAlias = rhs.getNamePath().getFirst();
      Table table = scope.getJoinScope(joinAlias).get();

      Relationship firstRel = (Relationship)table.getField(rhs.getNamePath().get(1));
      Name firstRelAlias = getTableAlias(rhs, 1);
      TableNode relation = new TableNode(Optional.empty(),
          firstRel.getToTable().getId().toNamePath(),
          Optional.of(firstRelAlias));

      TableBookkeeping b = new TableBookkeeping(relation, joinAlias, firstRel.getToTable());
      //Remaining
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
    } else {
      Table table = analyzer.getDag().getSchema().getByName(rhs.getNamePath().get(0)).get();
      Name joinAlias = getTableAlias(rhs, 0);
      TableNode tableNode = new TableNode(Optional.empty(), table.getName().toNamePath(), Optional.of(joinAlias));

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

  public static Optional<JoinCriteria> getCriteria(Relationship rel, Name alias, Name nextAlias) {
    List<Column> columns = rel.getTable().getPrimaryKeys();
    List<Expression> expr = columns.stream()
        .map(c->new ComparisonExpression(Optional.empty(), Operator.EQUAL, toIdentifier(c, alias), toIdentifier(c, nextAlias)))
        .collect(Collectors.toList());
    return Optional.of(new JoinOn(Optional.empty(), and(expr)));
  }

  private static Identifier toIdentifier(Column c, Name alias) {
    return new Identifier(Optional.empty(), alias.toNamePath().concat(c.getId().toNamePath()));
  }

  public static class JoinBuilder {
    List<Type> types = new ArrayList<>();
    List<Optional<JoinCriteria>> additional = new ArrayList<>();
    List<Field> fields = new ArrayList<>();
    List<NamePath> aliases = new ArrayList<>();
    List<Relation> relations = new ArrayList<>();

    public void add(Type joinType,
        Optional<JoinCriteria> additionalJoinCondition, Field field,
        NamePath alias, Relation relation) {
      types.add(joinType);
      additional.add(additionalJoinCondition);
      fields.add(field);
      aliases.add(alias);
      relations.add(relation);
    }

    public Relation build() {
      Relation current = new TableNode(Optional.empty(), fields.get(0).getId().toNamePath(),
          Optional.of(Name.system(aliases.get(0).toString())));

      for (int i = 1; i < fields.size(); i++) {
        current = new Join(Optional.empty(), types.get(i), current, relations.get(i), additional.get(i));
      }

      return current;
    }

    public Relation build(Relation node) {

      for (int i = 0; i < fields.size(); i++) {
        node = new Join(Optional.empty(), types.get(i), node, relations.get(i), additional.get(i));
      }

      return node;
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

  private Multimap<NamePath, Expression> extractNamedOutputExpressions(Select node)
  {
    // Compute aliased output terms so we can resolve order by expressions against them first
    Builder<NamePath, Expression> assignments = ImmutableMultimap.builder();
    for (SelectItem item : node.getSelectItems()) {
      if (item instanceof SingleColumn) {
        SingleColumn column = (SingleColumn) item;
        Optional<Identifier> alias = column.getAlias();
        if (alias.isPresent()) {
          assignments.put(alias.get().getNamePath(), column.getExpression()); // TODO: need to know if alias was quoted
        }
        else if (column.getExpression() instanceof Identifier) {
          assignments.put(((Identifier) column.getExpression()).getNamePath(), column.getExpression());
        }
      }
    }

    return assignments.build();
  }

  public void analyzeWhere(Node node, Scope scope, Expression predicate) {
    rewriteExpression(predicate, scope);
  }


  private List<Expression> analyzeOrderBy(Optional<OrderBy> orderBy,
      Scope scope) {
    if (orderBy.isEmpty()) return List.of();
    ImmutableList.Builder<Expression> orderByFieldsBuilder = ImmutableList.builder();


    for (SortItem item : orderBy.get().getSortItems()) {
      Expression expression = item.getSortKey();
      rewriteExpression(expression, scope);
      orderByFieldsBuilder.add(expression);
    }

    List<Expression> orderByFields = orderByFieldsBuilder.build();
    return orderByFields;
  }

  private Scope createScope(Node node, Scope parentScope) {
    return new Scope(parentScope.getContextTable(), node, parentScope.getJoinScope(), parentScope.getScopedRelation(),parentScope.getType(),parentScope.getCriteria(),
        parentScope.isExpression(), parentScope.getExpressionName());
  }

  private Scope createScope(Node node, Scope parentScope, Relation relation,
      Type type, JoinCriteria rewrittenCriteria) {
    return new Scope(parentScope.getContextTable(), node, parentScope.getJoinScope(), relation, type, rewrittenCriteria,
        parentScope.isExpression(), parentScope.getExpressionName());
  }

  private void analyzeHaving(QuerySpecification node, Scope scope) {
    if (node.getHaving().isPresent()) {
      Expression predicate = node.getHaving().get();
      Expression rewritten = rewriteExpression(predicate, scope);
    }
  }

  private Pair<List<SelectItem>, List<Expression>> analyzeSelect(QuerySpecification node, Scope scope) {
    List<Expression> outputExpressions = new ArrayList<>();

    List<SelectItem> selectItems = new ArrayList<>();

    //Add context keys
    if (scope.getContextTable().isPresent()) {
      Table table = scope.getJoinScope().get(Name.SELF_IDENTIFIER);
      List<Column> keys = table.getPrimaryKeys();
      selectItems.addAll(selectAlias(keys, Name.SELF_IDENTIFIER));
    }

    for (SelectItem item : node.getSelect().getSelectItems()) {
      if (item instanceof AllColumns) {
        Optional<Name> starPrefix = ((AllColumns) item).getPrefix()
            .map(e->e.getFirst());

        List<Field> fields = scope.resolveFieldsWithPrefix(starPrefix);

        for (Field field : fields) {
          Identifier identifier = new Identifier(item.getLocation(), field.getName().toNamePath());
          identifier.setResolved(FieldPath.of(field));
        }
      } else if (item instanceof SingleColumn) {
        SingleColumn column = (SingleColumn) item;
        Expression expression = rewriteExpression(column.getExpression(), scope);

        NamePath name;
        if (column.getAlias().isPresent()) {
          name = column.getAlias().get().getNamePath();
        } else if(column.getExpression() instanceof Identifier) {
          name = ((Identifier)column.getExpression()).getNamePath();
        } else {
          name = Name.system("expr$1").toNamePath();
        }

        selectItems.add( new SingleColumn(expression, new Identifier(Optional.empty(), name)));
      }
      else {
        throw new IllegalArgumentException(String.format("Unsupported SelectItem type: %s", item.getClass().getName()));
      }
    }

    return Pair.of(selectItems, outputExpressions);
  }

  private List<Expression> analyzeGroupBy(QuerySpecification node,
      Node rewritten, Scope scope,
      Pair<List<SelectItem>, List<Expression>> outputExpressions) {
    List<Expression> groupingExpressions = new ArrayList<>();

//
//
//    /*
//     * Adds context grouping keys
//     */
//    if (node.getGroupBy().isPresent() || isAggregating(rewritten)) {
//      Table table = scope.getContextTable().get();
//      for (Column column : table.getPrimaryKeys()) {
////        groupingExpressions.add(ident(Name.SELF_IDENTIFIER.toNamePath().concat(column.getId())));
//        groupingExpressions.add(new LongLiteral("0"));
//      }
//    }

    if (true) {
      return groupingExpressions;
    }
    if (node.getGroupBy().isEmpty()) {
      return null;
    }

    GroupingElement groupingElement = node.getGroupBy().get().getGroupingElement();
    for (Expression column : groupingElement.getExpressions()) {
      if (column instanceof LongLiteral) {
        throw new RuntimeException("Ordinals not supported in group by statements");
      }

      //if its an identifier: qualify identifier

      //if its not an identifier, find the column name and resolve it by ordinal

      //Group by statement must be one of the select fields
//      if (!(column instanceof Identifier)) {
//        log.info(
//            String.format("GROUP BY statement should use column aliases instead of expressions. %s",
//                column));
//        rewriteExpression(column, scope);
//        outputExpressions.stream()
//            .filter(e -> e.equals(column))
//            .findAny()
//            .orElseThrow(() -> new RuntimeException(
//                String.format("SELECT should contain GROUP BY expression %s", column)));
//        groupingExpressions.add(column);
//      }
    }

    return groupingExpressions;
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
