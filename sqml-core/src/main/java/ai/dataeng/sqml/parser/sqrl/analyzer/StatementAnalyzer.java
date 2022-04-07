package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.FieldPath;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Table;
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
import ai.dataeng.sqml.tree.NodeLocation;
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
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.util.Pair;

@Slf4j
public class StatementAnalyzer extends AstVisitor<Scope, Scope> {
  public final Analyzer analyzer;

  public final AliasGenerator gen = new AliasGenerator();

  public JoinBuilder joinBuilder = new JoinBuilder();

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

//    node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));
    List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
//    List<Expression> groupByExpressions = analyzeGroupBy(node, sourceScope, outputExpressions);
//    analyzeHaving(node, sourceScope);

    //analyzeOrderBy(node.getOrderBy(), sourceScope);

    //todo: check if there is a context query
//    Optional<GroupBy> groupBy = Optional.of(new GroupBy(Optional.empty(), new SimpleGroupBy(Optional.empty(), List.of())));

    QuerySpecification querySpecification = new QuerySpecification(
        node.getLocation(),
        toSelect(node.getSelect(), outputExpressions, List.of()),
        (Relation) joinFields((Relation) sourceScope.getNode()),
        node.getWhere(),
//        groupBy,
        node.getGroupBy(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );

    return createScope(querySpecification, scope);
  }

  private Relation joinFields(Relation node) {
    for (int i = 0; i < joinBuilder.fields.size(); i++) {
      node = new Join(Optional.empty(), joinBuilder.types.get(i), node, joinBuilder.relations.get(i), joinBuilder.additional.get(i));
    }

    return node;
  }

  private Select toSelect(Select select,
      List<Expression> groupByExpressions,
      List<Expression> outputExpressions) {
    List<SelectItem> items = Streams.concat(groupByExpressions.stream(), outputExpressions.stream())
        .map(e->new SingleColumn(Optional.empty(), e, Optional.empty()))
        .collect(Collectors.toList());

    return new Select(Optional.empty(), select.isDistinct(), items);
  }

  /**
   * Visits and expands a table
   */
  @Override
  public Scope visitTable(TableNode tableNode, Scope scope) {

    Pair<Table, Relation> rel = expandJoins(tableNode, scope);

    Name alias = tableNode.getAlias()
        .orElse(Name.system(tableNode.getNamePath().toString()));

    scope.getJoinScope().put(alias, rel.getKey());

    return createScope(rel.getValue(), scope);
  }

  /**
   * Expands a table path
   */
  private Pair<Table, Relation> expandJoins(TableNode tableNode, Scope scope) {
    NamePath tableName = tableNode.getNamePath();
    Table joinScope = scope.getJoinScope().get(tableName.getFirst());
    Optional<Table> baseTable =
        this.analyzer.getDag().getSchema().getByName(tableName.getFirst());
    if((joinScope == null && baseTable.isEmpty()
        || joinScope != null && baseTable.isPresent()) &&
        !tableName.getFirst().equals(Name.SELF_IDENTIFIER)) {
      throw new RuntimeException("Ambiguous table name: " + tableName);
    }


    Table table = null;
    Relation node = null;
    Name alias = null;
    if (joinScope != null) {
      //todo
    } else if (baseTable.isPresent()) {
      alias = tableNode.getNamePath().getLength() > 1
          ? tableNode.getAlias().orElse(Name.system(tableName.toString()))
          : baseTable.get().getName();

      node = new TableNode(Optional.empty(), baseTable.get().getId().toNamePath(), Optional.of(alias));
      table = baseTable.get();
    } else if (tableName.getFirst().equals(Name.SELF_IDENTIFIER)){
      alias = tableNode.getNamePath().getLength() == 1
          ? tableNode.getAlias().orElse(Name.SELF_IDENTIFIER)
          : Name.SELF_IDENTIFIER;

      node = new TableNode(Optional.empty(), scope.getContextTable().get().getId().toNamePath(), Optional.of(alias));
      table = scope.getContextTable().get();
    } else {
      throw new RuntimeException("unknown field");
    }

    if (tableName.getLength() > 1) {
      for (int i = 1; i < tableName.getNames().length; i++) {
        Name name = tableName.getNames()[i];
        //Inherit attributes for last element
        Name nextAlias;
        Optional<JoinCriteria> additionalCriteria;
        if (i == tableName.getNames().length - 1) {
          nextAlias = tableNode.getAlias().orElse(Name.system(tableName.toString()));
          additionalCriteria = scope.getAdditionalJoinCondition();
        } else {
          nextAlias = gen.nextTableAliasName();
          additionalCriteria = Optional.empty();
        }

        Relationship rel = (Relationship) table.getField(name);


        node = new Join(Optional.empty(), Type.INNER, node, getRelation(rel, nextAlias), getCriteria(rel, alias, nextAlias, additionalCriteria));

        table = rel.getToTable();
        alias = nextAlias;
      }
    }
    return Pair.of(table, node);
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

  public static Optional<JoinCriteria> getCriteria(Relationship rel, Name alias, Name nextAlias,
      Optional<JoinCriteria> additionalCriteria) {
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

    public Join join(NodeLocation location, Type joinType, Relation left, Relation right,
        Optional<Expression> condition) {
      Optional<JoinCriteria> joinCondition = condition.map(c -> new JoinOn(location, c));

      return new Join(location, joinType, left, right, joinCondition);
    }
  }

  @Override
  public Scope visitJoin(Join node, Scope scope) {
    Scope left = node.getLeft().accept(this, scope);
    Scope right = node.getRight().accept(this, left);

    if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT || node.getCriteria().isEmpty()) {
      //Add new scope to context for a context table and alias
    }

    JoinCriteria criteria = node.getCriteria().get();
    JoinCriteria rewrittenCriteria;
    if (criteria instanceof JoinOn) {
      Expression expression = ((JoinOn) criteria).getExpression();
      Expression rewriteExpression = rewriteExpression(expression, right);
      rewrittenCriteria = new JoinOn(((JoinOn) criteria).getLocation(), rewriteExpression);
    } else {
      throw new RuntimeException("Unsupported join");
    }

    Join rewrittenJoin =
        new Join(node.getLocation(), node.getType(), (Relation) left.getNode(), (Relation) right.getNode(), Optional.ofNullable(rewrittenCriteria));
    return createScope(rewrittenJoin, scope);
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
    ImmutableMultimap.Builder<NamePath, Expression> assignments = ImmutableMultimap.builder();
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
    return new Scope(parentScope.getContextTable(), node, parentScope.getJoinScope());
  }

  private void analyzeHaving(QuerySpecification node, Scope scope) {
    if (node.getHaving().isPresent()) {
      Expression predicate = node.getHaving().get();
      Expression rewritten = rewriteExpression(predicate, scope);
    }
  }

  private List<Expression> analyzeSelect(QuerySpecification node, Scope scope) {
    List<Expression> outputExpressions = new ArrayList<>();

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
        outputExpressions.add(rewriteExpression(column.getExpression(), scope));
//        outputExpressions.add(column.getExpression());
      }
      else {
        throw new IllegalArgumentException(String.format("Unsupported SelectItem type: %s", item.getClass().getName()));
      }
    }

    return outputExpressions;
  }

  private List<Expression> analyzeGroupBy(QuerySpecification node, Scope scope, List<Expression> outputExpressions) {
    List<Expression> groupingExpressions = new ArrayList<>();

    //Add grouping keys
    Table table = scope.getContextTable().get();
    for (Column column : table.getPrimaryKeys()) {
      groupingExpressions.add(new Identifier(Optional.empty(), Name.SELF_IDENTIFIER.toNamePath().concat(column.getId())));
    }

    if (node.getGroupBy().isEmpty()) {
      return groupingExpressions;
    }

    GroupingElement groupingElement = node.getGroupBy().get().getGroupingElement();
    for (Expression column : groupingElement.getExpressions()) {
      if (column instanceof LongLiteral) {
        throw new RuntimeException("Ordinals not supported in group by statements");
      }

      //Group by statement must be one of the select fields
      if (!(column instanceof Identifier)) {
        log.info(
            String.format("GROUP BY statement should use column aliases instead of expressions. %s",
                column));
        rewriteExpression(column, scope);
        outputExpressions.stream()
            .filter(e -> e.equals(column))
            .findAny()
            .orElseThrow(() -> new RuntimeException(
                String.format("SELECT should contain GROUP BY expression %s", column)));
        groupingExpressions.add(column);
      }
    }

    return groupingExpressions;
  }

  private Expression rewriteExpression(Expression expression, Scope scope) {
    ExpressionAnalyzer analyzer = new ExpressionAnalyzer(joinBuilder);
    Expression expr = analyzer.analyze(expression, scope);

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
