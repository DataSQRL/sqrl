package ai.datasqrl.plan.local.analyzer;

import static ai.datasqrl.parse.util.SqrlNodeUtil.isExpression;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.environment.ImportManager.TableImport;
import ai.datasqrl.function.FunctionMetadataProvider;
import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.function.calcite.CalciteFunctionMetadataProvider;
import ai.datasqrl.parse.tree.AliasedRelation;
import ai.datasqrl.parse.tree.AllColumns;
import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.Except;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Intersect;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.JoinDeclaration;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SetOperation;
import ai.datasqrl.parse.tree.SimpleGroupBy;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.parse.tree.SqrlStatement;
import ai.datasqrl.parse.tree.SubqueryExpression;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.TableSubquery;
import ai.datasqrl.parse.tree.Union;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.local.BundleTableFactory;
import ai.datasqrl.plan.local.analyzer.Analysis.ResolvedFunctionCall;
import ai.datasqrl.plan.local.analyzer.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.analyzer.Analysis.ResolvedNamedReference;
import ai.datasqrl.plan.local.analyzer.Analysis.TableVersion;
import ai.datasqrl.plan.local.analyzer.Analyzer.Scope;
import ai.datasqrl.plan.local.operations.SourceTableImportOp.RowType;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.RootTableField;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.TableStatistic;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import graphql.com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;

/**
 * Produces an Analysis object of a script/statement
 */
public class Analyzer extends DefaultTraversalVisitor<Scope, Scope> {

  private static final FunctionMetadataProvider functionMetadataProvider =
      new CalciteFunctionMetadataProvider(SqrlOperatorTable.instance());

  private final ImportManager importManager;
  private final SchemaAdjustmentSettings schemaSettings;
  private final ErrorCollector errors;
  private final boolean allowPaths = false;
  @Getter
  private final List<JoinNorm> addlJoins = new ArrayList<>();
  @Getter
  private final List<Expression> additionalColumns = new ArrayList<>();
  @Getter
  protected Analysis analysis;

  public Analyzer(ImportManager importManager, SchemaAdjustmentSettings schemaSettings,
      ErrorCollector errors) {
    this.importManager = importManager;
    this.schemaSettings = schemaSettings;
    this.errors = errors;
    this.analysis = new Analysis();
  }

  public Analysis analyze(ScriptNode script) {
    for (Node statement : script.getStatements()) {
      statement.accept(this, null);
    }
    return analysis;
  }

  public Analysis analyze(SqrlStatement statement) {
    statement.accept(this, null);
    return analysis;
  }

  @Override
  public Scope visitImportDefinition(ImportDefinition node, Scope context) {
    if (node.getNamePath().getLength() != 2) {
      throw new RuntimeException(
          String.format("Invalid import identifier: %s", node.getNamePath()));
    }

    //Check if this imports all or a single table
    Name sourceDataset = node.getNamePath().get(0);
    Name sourceTable = node.getNamePath().get(1);

    List<TableImport> importTables;
    Optional<Name> nameAlias;

    if (sourceTable.equals(ReservedName.ALL)) { //import all tables from dataset
      importTables = importManager.importAllTables(sourceDataset, schemaSettings, errors);
      nameAlias = Optional.empty();
    } else { //importing a single table
      //todo: Check if table exists
      SourceTableImport sourceTableImport = importManager.importTable(sourceDataset, sourceTable,
          schemaSettings, errors);
      importTables = List.of(sourceTableImport);
      nameAlias = node.getAliasName();
    }

    for (ImportManager.TableImport tblImport : importTables) {
      if (tblImport.isSource()) {
        SourceTableImport importSource = (SourceTableImport) tblImport;
        Map<Table, RowType> types = analysis.getSchema().addImportTable(importSource, nameAlias);
        analysis.getImportSourceTables().put(node, List.of(importSource));
        analysis.getImportTableTypes().put(node, types);

        //Plan timestamp expression.
//            schema.apply(new SourceTableImportOp(table, tableTypes, importSource));
//            NamePath colPath = NamePath.of(table.getName(), timeColName);
//            ExpressionAssignment timeColAssign = new ExpressionAssignment(Optional.empty(),
//            colPath,
//                timeColDef.getExpression(), "", List.of());
//            Visitor localVisitor = new Visitor(schema.peek());
//            AddColumnOp timeCol = localVisitor.planExpression(timeColAssign, colPath);
//            ops.add(timeCol.asTimestamp());
      } else {
        throw new UnsupportedOperationException("Script imports are not yet supported");
      }
    }

    return null;
  }

  @Override
  public Scope visitDistinctAssignment(DistinctAssignment node, Scope context) {
    Optional<Table> tableOpt = analysis.getSchema()
        .getTable(node.getTableNode().getNamePath().getFirst());
    Preconditions.checkState(tableOpt.isPresent());
    Preconditions.checkState(node.getTableNode().getNamePath().getLength() == 1);

    Table table = tableOpt.get();
    ResolvedNamePath resolvedNamePath = new ResolvedNamePath(node.getTableNode().getNamePath().getFirst().getCanonical(), Optional.empty(),
        List.of(new RootTableField(table)));
    analysis.getResolvedNamePath().put(node.getTableNode(), resolvedNamePath);

    Scope scope = new Scope(analysis.getSchema(), Optional.empty(), Optional.empty(),
        Optional.empty(), null);
    scope.joinScopes.put(table.getName(), resolvedNamePath);

    node.getPartitionKeyNodes().forEach(pk -> pk.accept(this, scope));

    node.getOrder().forEach(o -> o.accept(this, scope));

    //Add to schema:
    double derivedRowCount = 1; //TODO: derive from optimizer
    final BundleTableFactory.TableBuilder builder = analysis.getSchema().getTableFactory()
        .build(node.getTableNode().getNamePath());
    table.getVisibleColumns().forEach(
        c -> builder.addColumn(c.getName(), c.isPrimaryKey(), c.isParentPrimaryKey(),
            c.isVisible()));
    Table.Type tblType = Table.Type.STREAM;
    Table distinctTable = builder.createTable(tblType, TableStatistic.of(derivedRowCount));

    analysis.getSchema().add(distinctTable);
    analysis.getProducedTable().put(node, distinctTable.getCurrentVersion());

    return null;
  }

  @Override
  public Scope visitJoinAssignment(JoinAssignment node, Scope context) {
    //1. Validate all entries
    Scope scope = createScope(node.getNamePath(), false, null);

    addSelfToScope(scope, scope.getContextTable());

    node.getJoinDeclaration().accept(this, scope);

    //TargetTable:
    TableNode targetTableNode = node.getJoinDeclaration().getRelation() instanceof TableNode
        ? (TableNode) node.getJoinDeclaration().getRelation()
        : (TableNode) ((Join) node.getJoinDeclaration()
            .getRelation()).getRight(); //assumption: always a join list

    ResolvedNamePath resolvedTable = analysis.getResolvedNamePath().get(targetTableNode);

    //Add to schema:
    Table parentTable = analysis.getSchema().walkTable(node.getNamePath().popLast());
    Multiplicity multiplicity = node.getJoinDeclaration().getLimit().map(
        l -> l.getIntValue().filter(i -> i == 1).map(i -> Multiplicity.ONE)
            .orElse(Multiplicity.MANY)).orElse(Multiplicity.MANY);
    Relationship relationship = new Relationship(node.getNamePath().getLast(), parentTable,
        resolvedTable.getToTable(), JoinType.JOIN, multiplicity, null,
        //Todo: not stored here, store in calcite to resolve table paths (maybe materialize)
        Optional.empty(), node.getJoinDeclaration().getLimit());

    parentTable.getFields().add(relationship);
    analysis.getProducedField().put(node, relationship);

    return null;
  }

  @Override
  public Scope visitExpressionAssignment(ExpressionAssignment node, Scope context) {
    Scope scope = createScope(node.getNamePath(), true, null);
    scope.setSelfInScope(true);
    addSelfToScope(scope, scope.getContextTable());
    node.getExpression().accept(this, scope);

    // Add to schema
    Table table = scope.getContextTable().get();
    Name columnName = node.getNamePath().getLast();
    int nextVersion = table.getNextColumnVersion(columnName);

    Column column = new Column(columnName, nextVersion, table.getNextColumnIndex(), false, false,
        true);

    table.addExpressionColumn(column);
    analysis.getProducedTable().put(node, table.getCurrentVersion());
    analysis.getProducedField().put(node, column);

    return null;
  }

  /**
   * Assigns a query to a variable.
   * <p>
   * Table := SELECT * FROM x;
   * <p>
   * Query may assign a column instead of a query if there is a single unnamed or same named column
   * and it does not break the cardinality of the result.
   * <p>
   * Table.column := SELECT productid + 1 FROM _;  <==> Table.column := productid + 1;
   * <p>
   * Table.total := SELECT sum(productid) AS total FROM _ HAVING total > 10;
   */
  @Override
  public Scope visitQueryAssignment(QueryAssignment queryAssignment, Scope context) {
    NamePath namePath = queryAssignment.getNamePath();
    Query query = queryAssignment.getQuery();

    boolean isExpression = isExpression(queryAssignment.getQuery());
    if (isExpression) {
      analysis.getExpressionStatements().add(queryAssignment);
    }

    Scope scope = createScope(namePath, isExpression, query);

    Scope queryScope = query.accept(this, scope);

    if (isExpression) {
      //do not create table, add column
      Table table = analysis.getSchema().walkTable(namePath.popLast());
      Name columnName = namePath.getLast();
      int nextVersion = table.getNextColumnVersion(columnName);

      Column column = new Column(columnName, nextVersion, table.getNextColumnIndex(), false, false,
          true);
      table.getFields().add(column);
      analysis.getProducedField().put(queryAssignment, column);
      analysis.getProducedTable().put(queryAssignment, table.getCurrentVersion());
    } else {
      double derivedRowCount = 1; //TODO: derive from optimizer

      final BundleTableFactory.TableBuilder builder = analysis.getSchema().getTableFactory()
          .build(namePath);

      //todo: column names:
      queryScope.getFieldNames().forEach(n -> builder.addColumn(n, false, false, true));

      //TODO: infer table type from relNode
      Table.Type tblType = Table.Type.STREAM;

      //Creates a table that is not bound to the schema TODO: determine timestamp
      Table table = builder.createTable(tblType, TableStatistic.of(derivedRowCount));

      if (namePath.getLength() == 1) {
        analysis.getSchema().add(table);
      } else {
        Table parentTable = analysis.getSchema().walkTable(namePath.popLast());
        Name relationshipName = namePath.getLast();
        Relationship.Multiplicity multiplicity = Multiplicity.MANY;
//        if (specNorm.getLimit().flatMap(Limit::getIntValue).orElse(2) == 1) {
//          multiplicity = Multiplicity.ONE;
//        }

        Optional<Relationship> parentRel = analysis.getSchema().getTableFactory()
            .createParentRelationship(table, parentTable);
        parentRel.map(rel -> table.getFields().add(rel));

        Relationship childRel = analysis.getSchema().getTableFactory()
            .createChildRelationship(relationshipName, table, parentTable, multiplicity);
        parentTable.getFields().add(childRel);
      }
      analysis.getProducedTable().put(queryAssignment, new TableVersion(table, 0));
    }

    return null;
  }

  @Override
  public Scope visitJoinDeclaration(JoinDeclaration node, Scope context) {
    Scope relScope = node.getRelation().accept(this, context);

    node.getOrderBy().map(o -> o.accept(this, relScope));
    node.getInverseNode().map(i -> i.accept(this, relScope));

    return relScope;
  }

  /**
   * SET operations define a limit & an order on the outside:
   *
   * <p>
   * SELECT * FROM Product
   * <p>
   * UNION
   * <p>
   * SELECT * FROM Product
   * <p>
   * LIMIT 5; <-- refers to limit on set
   * </p>
   */
  @Override
  public Scope visitQuery(Query node, Scope scope) {
    Scope queryBodyScope = node.getQueryBody().accept(this, scope);
    if (node.getOrderBy().isPresent()) {
      node.getOrderBy().map(o -> o.accept(this, scope));
      //Disallow order & limit in union statements (?)

//      if (queryBodyScope.getOuterQueryParent().isPresent() && !node.getLimit().isPresent()) {
//         not the root scope and ORDER BY is ineffective
//        analysis.markRedundantOrderBy(node.getOrderBy().get());
//        warningCollector.add(
//            new PrestoWarning(REDUNDANT_ORDER_BY, "ORDER BY in subquery may have no effect"));
//      }
    }

    return queryBodyScope;
  }

  /**
   * QuerySpecification contains what you expect in a query.
   * <p>
   * 1. Order/group/having should refer to its alias or issue a warning 2.
   */
  @Override
  public Scope visitQuerySpecification(QuerySpecification node, Scope scope) {
    boolean isSelfInScope = hasExplicitSelfTable(node.getFrom());
    /* Desugar self joins. If we're nested, we need to join a self table if one is not present. */
    if (scope.getContextTable().isPresent()) {
      if (!isSelfInScope) {
        analysis.getNeedsSelfTableJoin().add(node);
      }
      addSelfToScope(scope, scope.getContextTable());
    }

    Scope relScope = node.getFrom().accept(this, scope);

    analysis.getScopes().put(node, relScope);
    node.getSelect().accept(this, relScope);
    node.getWhere().map(w -> w.accept(this, relScope));

    //Rescope for group/having/order
//    List<SingleColumn> expandSelect = scope.expandSelect(node.getSelect().getSelectItems(),
//    scope);
//    analysis.getSelectExpressions().put(node, expandSelect);
    //TODO: If there is a select *, store the visible fields in the current scope.
    // Alternatively, keep the scope around and resolve it later (better)

    //TODO: Group by/having/order must refer to an item in the expanded select list. Repeated
    // select list items should be met with a warning in the future, currently disallow them.
    // This is because it could contain local aggregate statements that aren't expandable here
    // It's not hard, the logic in the git history somewhere
    node.getGroupBy().map(g -> g.accept(this, relScope));
    //Todo: Having must use select list scope.
//    node.getHaving().map(h -> h.accept(this, relScope));
    node.getOrderBy().map(o -> o.accept(this, relScope));

    return relScope;
  }

  private void addSelfToScope(Scope scope, Optional<Table> contextTable) {
    ResolvedNamePath resolvedNamePath = new ResolvedNamePath("_", Optional.empty(),
        List.of(new RootTableField(scope.getContextTable().get())));
    scope.joinScopes.put(ReservedName.SELF_IDENTIFIER, resolvedNamePath);
  }

  @Override
  public Scope visitSelect(Select node, Scope context) {
    List<Name> selectNames = new ArrayList<>();

    for (SelectItem selectItem : node.getSelectItems()) {

      if (selectItem instanceof SingleColumn) {
        selectNames.add(getSelectItemName((SingleColumn) selectItem, context));
      } else if (selectItem instanceof AllColumns) {
        if (((AllColumns) selectItem).getPrefix().isPresent()) {
          //Wrong, need to uniquely alias columns
          List<Identifier> identifiers = context.resolveFieldsWithPrefix(
              ((AllColumns) selectItem).getPrefix().map(p -> p.getLast()));
          selectNames.addAll(identifiers.stream().map(i -> i.getNamePath().getLast())
              .collect(Collectors.toList()));
        }
      } else {
        throw new RuntimeException();
      }

    }
    context.setFieldNames(selectNames);
    return super.visitSelect(node, context);
  }

  @Override
  public Scope visitSimpleGroupBy(SimpleGroupBy node, Scope scope) {
    Scope groupByScope = createGroupOrSortScope(scope);
    node.getExpressions().forEach(e -> e.accept(this, groupByScope));

    return groupByScope;
  }

  private Scope createGroupOrSortScope(Scope scope) {
    Scope newScope = new Scope(scope.schema, scope.contextTable, scope.isExpression, scope.targetName, scope.query);
    newScope.setSelfInScope(scope.isSelfInScope);
    newScope.setFieldNames(scope.fieldNames);
    newScope.isInGroupByOrSortBy = true;
    newScope.joinScopes.putAll(scope.getJoinScopes());
    return newScope;
  }

  @Override
  public Scope visitOrderBy(OrderBy node, Scope scope) {
    Scope orderByScope = createGroupOrSortScope(scope);

    node.getSortItems().stream().map(SortItem::getSortKey).forEach(e -> e.accept(this, orderByScope));
    return null;
  }

  @Override
  public Scope visitTableSubquery(TableSubquery node, Scope scope) {

    return node.getQuery().accept(this, scope);
    //add subqueries to analyzer so they can be materialized
    //(What about subquery expressions?)

//    Scope subQueryScope = createEmptyScope(scope.getSchema());
//    Scope norm = node.getQuery().accept(this, subQueryScope);
//
//    scope.getJoinScopes().put(Name.system("_subquery_" + scope.getInternalIncrementer()
//    .incrementAndGet()), norm);
  }

  @Override
  public Scope visitAliasedRelation(AliasedRelation node, Scope scope) {
    Scope subQueryScope = createEmptyScope(scope.getSchema());
    Scope norm = node.getRelation().accept(this, subQueryScope);

//    scope.getJoinScopes().put(node.getAlias().getNamePath().getLast(), norm);
    return norm;
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
  public Scope visitSetOperation(SetOperation node, Scope context) {
    node.getRelations().forEach(e -> e.accept(this, context));
    return null;
  }

  @Override
  public Scope visitJoin(Join node, Scope scope) {
    node.getLeft().accept(this, scope);
    node.getRight().accept(this, scope);

    node.getCriteria().map(j -> j.accept(this, scope));
    return scope;
  }

  @Override
  public Scope visitJoinOn(JoinOn node, Scope context) {
    node.getExpression().accept(this, context);
    return context;
  }

  /**
   * Expands table identifiers.
   * <p>
   * Local scoped tables are expanded when they are first referenced
   */
  @Override
  public Scope visitTableNode(TableNode node, Scope scope) {
    if (node.getNamePath().getLength() == 1 && node.getNamePath().getFirst()
        .equals(ReservedName.SELF_IDENTIFIER)) {
      scope.setSelfInScope(true);
    }

    ResolvedNamePath table = scope.resolveTableOrThrow(node.getNamePath());

    analysis.getResolvedNamePath().put(node, table);

    Name alias = getTableName(node, scope);
    scope.getJoinScopes().put(alias, table);
    analysis.getTableAliases().put(node, alias);

    return scope;
  }

  private Name getSelectItemName(SingleColumn column, Scope scope) {
    if (column.getAlias().isPresent()) {
      return column.getAlias().get().getNamePath().getLast();
    } else if (column.getExpression() instanceof Identifier) {
      return ((Identifier) column.getExpression()).getNamePath().getLast();
    } else if (scope.getIsExpression().isPresent() && scope.getIsExpression().get()) {
      return scope.getTargetName().get();
    }
    return Name.system("_expr");
  }
//
//  private List<SortItem> analyzeOrderBy(List<SingleColumn> selectList, Optional<OrderBy> orders) {
//    if (orders.isEmpty()) {
//      return new ArrayList<>();
//    }
//    List<SortItem> newSortItems = new ArrayList<>();
//    for (SortItem sortItem : orders.get().getSortItems()) {
//      //Look at the alias name, if its the alias name then return ordinal
//      //Look in select list, if the identifier is in there exactly then return ordinal
//      //Else: rewrite expression
//      int ordinal = getSelectListOrdinal(selectList, sortItem.getSortKey());
//      if (ordinal != -1) {
//        newSortItems.add(new SortItem(sortItem.getLocation(), new ReferenceOrdinal(ordinal),
//            sortItem.getOrdering()));
//      } else {
//        newSortItems.add(sortItem);
//      }
//    }
//
//    return newSortItems;
//  }
//
//  private int getSelectListOrdinal(List<SingleColumn> selectList, Expression expression) {
//    //Check in selectlist for alias
//    for (int i = 0; i < selectList.size(); i++) {
//      SingleColumn item = selectList.get(i);
//      if (item.getAlias().isPresent() && item.getAlias().get().equals(expression)) {
//        return i;
//      }
//    }
//
//    return selectList.stream().map(e -> e.getExpression()).collect(Collectors.toList())
//        .indexOf(expression);
//  }
//
//  private Optional<Expression> analyzeHaving(QuerySpecification node, Scope scope,
//      List<SingleColumn> selectList) {
//    if (node.getHaving().isPresent()) {
//      return Optional.of(rewriteHavingExpression(node.getHaving().get(), selectList, scope));
//    }
//    return Optional.empty();
//  }
//
//  private Expression rewriteHavingExpression(Expression expression, List<SingleColumn> selectList,
//      Scope scope) {
//    List<Expression> expressionList = selectList.stream().map(s -> s.getExpression())
//        .collect(Collectors.toList());
//
//    Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<>() {
//      @Override
//      public Expression rewriteFunctionCall(FunctionCall node, List<SingleColumn> context,
//          ExpressionTreeRewriter<List<SingleColumn>> treeRewriter) {
//        int index = expressionList.indexOf(node);
//        if (index != -1) {
//          return new ReferenceOrdinal(index);
//        }
//
//        return node;
//      }
//    }, expression, selectList);
//    //remove expressions that are in the select list and replace them with identifiers. The
//    // replacing identifier is always a FunctionCall.
//
//    //Also, qualify all of the fields, disallow paths.
//    return rewriteExpression(rewritten, false, scope);
//  }
//
//  private Expression rewriteExpression(Expression expression, boolean allowPaths, Scope scope) {
////    ExpressionNormalizer expressionNormalizer = new ExpressionNormalizer(allowPaths);
////    Expression rewritten = ExpressionTreeRewriter.rewriteWith(expressionNormalizer, expression,
////    scope);
////    scope.getAddlJoins().addAll(expressionNormalizer.getAddlJoins());
//    return null;
//  }
//
//  protected Optional<OrderBy> rewriteOrderBy(Optional<OrderBy> orderBy, Scope scope) {
//    if (orderBy.isEmpty()) {
//      return Optional.empty();
//    }
//
//    List<SortItem> sortItems = new ArrayList<>();
//    for (SortItem sortItem : orderBy.get().getSortItems()) {
//      Expression key = rewriteExpression(sortItem.getSortKey(), scope);
//      sortItems.add(new SortItem(sortItem.getLocation(), key, sortItem.getOrdering()));
//    }
//
//    OrderBy order = new OrderBy(orderBy.get().getLocation(), sortItems);
//    return Optional.of(order);
//  }

//  private Expression rewriteExpression(Expression expression, Scope scope) {
//    return ExpressionTreeRewriter.rewriteWith(new ExpressionNormalizer(false), expression, scope);
//    return null;
//  }

  @Override
  public Scope visitIdentifier(Identifier node, Scope scope) {
    ResolvedNamePath field = scope.resolveNamePathOrThrow(node.getNamePath());

    //3. If !allowPaths and is path, error and return
//    if (path.getLength() > 1 && !allowPaths) {
//      throw new RuntimeException("Paths encountered where path is not allowed");
//    }
//
//    if (isToMany(field)) {
//      throw new RuntimeException("To-many relationship not expected here:" + path);
//    }

    analysis.getResolvedNamePath().put(node, field);
    return scope;
  }

  @Override
  public Scope visitFunctionCall(FunctionCall node, Scope scope) {
    Optional<SqrlAwareFunction> functionOptional = functionMetadataProvider.lookup(
        node.getNamePath());
    Preconditions.checkState(functionOptional.isPresent(), "Could not find function {}",
        node.getNamePath());
    SqrlAwareFunction function = functionOptional.get();
    analysis.getResolvedFunctions().put(node, new ResolvedFunctionCall(function));

    if (function.isAggregate() && node.getArguments().size() == 1 && node.getArguments()
        .get(0) instanceof Identifier) {
      Identifier identifier = (Identifier) node.getArguments().get(0);

      ResolvedNamePath path = scope.resolveNamePathOrThrow(identifier.getNamePath());
      analysis.getResolvedNamePath().put(identifier, path);
      if (path.getPath().size() > 1 && isToMany(path)) {
        analysis.getIsLocalAggregate().add(node);
        return null;
      }
    }

    for (Expression arg : node.getArguments()) {
      arg.accept(this, scope);
    }

    node.getOver().map(over -> over.accept(this, scope));

    return scope;
  }

  private boolean isToMany(ResolvedNamePath fields) {
    if (fields.getPath().isEmpty()) {
      return false;
    }
    for (Field field : fields.getPath()) {
      if (field instanceof Relationship
          && ((Relationship) field).getMultiplicity() != Multiplicity.MANY) {
        return false;
      }
    }
    return true;
  }

  /**
   * Has a self table as a single defined table:
   * <p>
   * FROM _;
   * <p>
   * Not: FROM _.entries;
   */
  private boolean hasExplicitSelfTable(Relation node) {
    AtomicBoolean atomicBoolean = new AtomicBoolean(false);
    node.accept(new DefaultTraversalVisitor<>() {
      //Don't walk subqueries
      @Override
      public Object visitSubqueryExpression(SubqueryExpression node, Object context) {
        return null;
      }

      @Override
      public Object visitTableSubquery(TableSubquery node, Object context) {
        return null;
      }

      @Override
      public Object visitTableNode(TableNode node, Object context) {
        if (node.getNamePath().getLength() == 1 && node.getNamePath().getFirst()
            .equals(ReservedName.SELF_IDENTIFIER)) {
          atomicBoolean.set(true);
        }
        return null;
      }
    }, null);

    return atomicBoolean.get();
  }

  private Scope createEmptyScope(Schema schema) {
    return new Scope(schema, Optional.empty(), Optional.empty(), Optional.empty(), null);
  }

  private Name getTableName(TableNode node, Scope scope) {
    return node.getAlias()
        .or(() -> node.getNamePath().getLength() == 1 ? Optional.of(node.getNamePath().getFirst())
            : Optional.empty())
        //If we're in a table path, the fields cannot be referenced using the path syntax
        .orElseGet(
            () -> Name.system("_internal$" + scope.getInternalIncrementer().incrementAndGet()));
  }

  private Optional<Table> getContext(NamePath namePath) {
    if (namePath.getLength() == 0) {
      return Optional.empty();
    }
    Table table = analysis.getSchema().getVisibleByName(namePath.getFirst()).get();
    return table.walkTable(namePath.popFirst());
  }

  private Scope createScope(NamePath namePath, boolean isExpression, Query query) {
    Optional<Table> contextTable = getContext(namePath.popLast());
    return new Scope(analysis.getSchema(), contextTable, isExpression, namePath.getLast(), query);
  }

  @Getter
  public class Scope {

    private final Schema schema;
    private final Optional<Table> contextTable;
    private final Optional<Boolean> isExpression;
    private final Optional<Name> targetName;

    /**
     * Fields available for resolution.
     */
    private final Map<Name, ResolvedNamePath> joinScopes = new HashMap<>();
    //Used to create internal joins that cannot be unreferenced by alias
    private final AtomicInteger internalIncrementer = new AtomicInteger();
    private final AtomicBoolean hasExpandedSelf = new AtomicBoolean();
    private final List<JoinNorm> addlJoins = new ArrayList<>();
    private final Map<RelationNorm, Name> aliasRelationMap = new HashMap<>();
    private final Query query;

    /**
     * Query has a context table explicitly defined in the query. Self fields are always available
     * but do not cause ambiguity problems when fields are referenced without aliases.
     * <p>
     * e.g. FROM _ JOIN _.orders; vs FROM _.orders;
     */
    @Setter
    private boolean isSelfInScope = false;
    @Setter
    private List<Name> fieldNames = new ArrayList<>();
    @Setter
    private boolean isInGroupByOrSortBy = false;

    public Scope(Schema schema, Optional<Table> contextTable, boolean isExpression, Name targetName,
        Query query) {
      this(schema, contextTable, Optional.of(isExpression), Optional.of(targetName), query);
    }

    public Scope(Schema schema, Optional<Table> contextTable, Optional<Boolean> isExpression,
        Optional<Name> targetName, Query query) {
      this.schema = schema;
      this.contextTable = contextTable;
      this.isExpression = isExpression;
      this.targetName = targetName;
      this.query = query;
    }

    /**
     * Expands SELECT *
     */
//    public List<SingleColumn> expandSelect(List<SelectItem> selectItems, Scope scope) {
//      //     includeSelfWithSelectStar = false;
//      List<SingleColumn> expressions = new ArrayList<>();
//      for (SelectItem item : selectItems) {
//        if (item instanceof AllColumns) {
//          Optional<Name> alias = ((AllColumns) item).getPrefix().map(e -> e.getFirst());
//          List<Expression> fields = resolveFieldsWithPrefix(alias);
//          for (Expression expr : fields) {
//            expressions.add(new SingleColumn(Optional.empty(), expr, Optional.empty()));
//          }
//        } else {
//          SingleColumn col = (SingleColumn) item;
//          if (col.getAlias().isEmpty()) {
//            if (col.getExpression() instanceof Identifier) {
//              expressions.add(new SingleColumn(col.getExpression(), Optional.empty()));
//            } else {
//              //Try to name the column something reasonable so table headers are readable
//              if (scope.isExpression.isPresent() && scope.getIsExpression().get()) {
//                Name nextName = scope.getContextTable().get()
//                    .getNextFieldId(scope.getTargetName().get());
//                expressions.add(
//                    new SingleColumn(col.getExpression(), Optional.of(new Identifier(nextName))));
//              } else {
//                log.warn("Column missing a derivable alias:" + col.getExpression());
//                expressions.add(new SingleColumn(col.getExpression(), Optional.empty()));
//              }
//            }
//          } else {
//            expressions.add(col);
//          }
//        }
//      }
//      return expressions;
//    }
    private List<Identifier> resolveFieldsWithPrefix(Optional<Name> alias) {
//      if (alias.isPresent()) {
//       ResolvedTable norm = joinScopes.get(alias.get());
//        Preconditions.checkNotNull(norm, "Could not find table %s", alias.get());
//       List<Identifier> exprs = norm.getTableVersion().getTable().getFields().stream()
//          .map(f -> new Identifier(Optional.empty(), f.getName().toNamePath()))
//          .collect(Collectors.toList());
//        return exprs;
//      }
//
//      List<Identifier> allFields = new ArrayList<>();
//      for (Map.Entry<Name, ResolvedTable> entry : joinScopes.entrySet()) {
////        if (isSelfTable(entry.getValue()) && !isSelfInScope) {
////          continue;
////        }
//
//      Table table = entry.getValue().getTableVersion().getTable();
//      List<Identifier> identifiers = table.getVisibleColumns().stream()
//          .map(f -> new Identifier(Optional.empty(),
//              entry.getKey().toNamePath().concat(f.getName())))
//          .collect(Collectors.toList());
//        allFields.addAll(identifiers);
//      }
//
//      return allFields;
      return null;
    }

    private boolean isSelfTable(RelationNorm norm) {
      return norm instanceof TableNodeNorm && ((TableNodeNorm) norm).isLocalTable();
    }

    public List<Name> getFieldNames() {
      return fieldNames;
    }

    public ResolvedNamePath resolveTableOrThrow(NamePath namePath) {
      Optional<List<ResolvedNamePath>> paths = resolveTable(namePath);
      List<ResolvedNamePath> path = paths.orElseThrow(
          () -> new RuntimeException("Cannot find path: " + namePath));
      if (path.size() == 0) {
        throw new RuntimeException("Cannot find path: " + namePath);
      } else if (path.size() > 1) {
        throw new RuntimeException("Ambiguous path: " + namePath);
      }
      return path.get(0);
    }

    public ResolvedNamePath resolveNamePathOrThrow(NamePath namePath) {
      Optional<List<ResolvedNamePath>> paths = resolveNamePath(namePath);
      List<ResolvedNamePath> path = paths.orElseThrow(
          () -> new RuntimeException("Cannot find path: " + namePath));
      if (path.size() == 0) {
        throw new RuntimeException("Cannot find path: " + namePath);
      } else if (path.size() > 1) {
        throw new RuntimeException("Ambiguous path: " + namePath);
      }
      return path.get(0);
    }

    //Tables have one extra requirement, they must have an alias for relative table names
    public Optional<List<ResolvedNamePath>> resolveTable(NamePath namePath) {
      return resolveNamePath(namePath, true);
    }

    public Optional<List<ResolvedNamePath>> resolveNamePath(NamePath namePath) {
      return resolveNamePath(namePath, false);
    }

    public Optional<List<ResolvedNamePath>> resolveNamePath(NamePath namePath,
        boolean requireAlias) {
      if (isInGroupByOrSortBy && namePath.getLength() == 1) { //select list items take priority
        for (int i = 0; i < fieldNames.size(); i++) {
          Name name = fieldNames.get(i);
          if (namePath.get(0).equals(name)) {
            //todo: need to resolve the entire identifier
            return Optional.of(List.of(new ResolvedNamedReference(name, i)));
          }
        }
      }

      List<ResolvedNamePath> resolved = new ArrayList<>();
      Optional<ResolvedNamePath> explicitAlias = Optional.ofNullable(
          joinScopes.get(namePath.getFirst()));

      if (explicitAlias.isPresent() && (namePath.getLength() == 1 || walk(
          namePath.getFirst().getCanonical(), explicitAlias.get(),
          namePath.popFirst()).isPresent())) { //Alias take priority
        if (namePath.getLength() == 1) {
          return explicitAlias.map(List::of);
        } else {
          resolved.add(walk(namePath.getFirst().getCanonical(), explicitAlias.get(), namePath.popFirst()).get());
          return Optional.of(resolved);
        }
      }

      // allows resolution from join scopes
      if (!requireAlias) {
        for (Map.Entry<Name, ResolvedNamePath> entry : joinScopes.entrySet()) {
          //Avoid conditions where columns may be ambiguous if self table is not declared explicitly
          if (entry.getKey().equals(ReservedName.SELF_IDENTIFIER) && !isSelfInScope()) {
            continue;
          }

          Optional<ResolvedNamePath> resolvedPath = walk(entry.getKey().getCanonical(), entry.getValue(), namePath);
          resolvedPath.ifPresent(resolved::add);
        }
      }

      Optional<ResolvedNamePath> path = walkSchema(namePath);
      path.ifPresent(resolved::add);

      return resolved.isEmpty() ? Optional.empty() : Optional.of(resolved);
    }

    private Optional<ResolvedNamePath> walkSchema(NamePath namePath) {
      Optional<Table> table = schema.getTable(namePath.getFirst());
      List<Field> fields = new ArrayList<>();
      if (table.isPresent()) {
        Field root = new RootTableField(table.get());
        fields.add(root);
        for (Name name : namePath.popFirst()) {
          if (table.isEmpty()) {
            return Optional.empty();
          }
          Optional<Field> field = table.get().getField(name);
          if (field.isEmpty()) {
            return Optional.empty();
          }
          fields.add(field.get());
          table = field.flatMap(this::getTableOfField);
        }
      } else {
        return Optional.empty();
      }

      return Optional.of(new ResolvedNamePath(namePath.getFirst().getCanonical(), Optional.empty(), fields));
    }

    private Optional<ResolvedNamePath> walk(String alias, ResolvedNamePath resolvedNamePath, NamePath namePath) {
      Field field = resolvedNamePath.getPath().get(resolvedNamePath.getPath().size() - 1);
      Optional<Table> table = getTableOfField(field);
      if (table.isEmpty()) {
        return Optional.empty();
      }

      Optional<List<Field>> fields = walk(table.get(), namePath);
      if (fields.isEmpty()) {
        return Optional.empty();
      }

      return Optional.of(new ResolvedNamePath(alias, Optional.of(resolvedNamePath), fields.get()));
    }

    private Optional<List<Field>> walk(Table tbl, NamePath namePath) {
      Optional<Table> table = Optional.of(tbl);
      List<Field> fields = new ArrayList<>();
      for (Name name : namePath) {
        if (table.isEmpty()) {
          return Optional.empty();
        }
        Optional<Field> field = table.get().getField(name);
        if (field.isEmpty()) {
          return Optional.empty();
        }
        fields.add(field.get());
        table = field.flatMap(this::getTableOfField);
      }

      return Optional.of(fields);
    }

    private Optional<Table> getTableOfField(Field field) {
      if (field instanceof RootTableField) {
        return Optional.of(((RootTableField) field).getTable());
      } else if (field instanceof Relationship) {
        return Optional.of(((Relationship) field).getToTable());
      } else {
        return Optional.empty();
      }
    }
  }
}
