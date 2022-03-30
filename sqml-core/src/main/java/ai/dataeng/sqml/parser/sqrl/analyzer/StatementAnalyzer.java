package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.FieldPath;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.SelfField;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.TableField;
import ai.dataeng.sqml.parser.sqrl.analyzer.ExpressionAnalyzer.ExpressionAnalysis;
import ai.dataeng.sqml.tree.AllColumns;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.ComparisonExpression.Operator;
import ai.dataeng.sqml.tree.Except;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionRewriter;
import ai.dataeng.sqml.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.GroupingElement;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Intersect;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.Join.Type;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.Limit;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeLocation;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SetOperation;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatementAnalyzer extends AstVisitor<Scope, Scope> {
  private final Analyzer analyzer;
  private final Multimap<AnalyzerTable, FunctionCall> toManyFields = ArrayListMultimap.create();
  private final Multimap<AnalyzerTable, FieldPath> toOneFields = HashMultimap.create();
  private final Multimap<FieldPath, Identifier> toOneMapping = HashMultimap.create();

  private final Map<Node, Node> nodeMapper = new HashMap<>();
  private final AliasGenerator gen = new AliasGenerator();
  private final AtomicBoolean hasContext = new AtomicBoolean();
  private Map<TableNode, AnalyzerTable> analyzerTableMap = new HashMap<>();

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

//    if (node.getOrderBy().isPresent()) {
//      List<Expression> orderByExpressions = analyzeOrderBy(node, getSortItemsFromOrderBy(node.getOrderBy()), queryBodyScope);
//    }

    return createAndAssignScope(node, scope, (List<AnalyzerField>)queryBodyScope.getFields());
  }

  @Override
  public Scope visitQuerySpecification(QuerySpecification node, Scope scope) {
    Scope sourceScope = node.getFrom().accept(this, scope);

    node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));
    List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
    List<Expression> groupByExpressions = analyzeGroupBy(node, sourceScope, outputExpressions);
    analyzeHaving(node, sourceScope);
    
    Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);
//
//    List<Expression> orderByExpressions = new ArrayList<>();
//    Optional<Scope> orderByScope = Optional.empty();
//    if (node.getOrderBy().isPresent()) {
//      if (node.getSelect().isDistinct()) {
//        verifySelectDistinct(node, outputExpressions);
//      }
//
//      OrderBy orderBy = node.getOrderBy().get();
//      orderByScope = Optional.of(computeAndAssignOrderByScope(orderBy, sourceScope, outputScope, node));
//
    analyzeOrderBy(node.getOrderBy(), sourceScope);
//    }

    Optional<Long> limit = node.parseLimit();

    // Unsqrl the node
    //Distinct becomes primary key hint
    if (node.getSelect().isDistinct()) {

    }

    QuerySpecification query = new QuerySpecification(
        node.getLocation(),
        rewriteSelect(node.getSelect()),
        rewriteFrom(node.getFrom()),
        rewriteWhere(node.getWhere()),
        rewriteGroupBy(node.getGroupBy()),
        rewriteHaving(node.getHaving()),
        rewriteOrderBy(node.getOrderBy()),
        rewriteLimit(node.getLimit())
    );

    return new Scope(Optional.empty(), query, null);
  }

  private Relation rewriteFrom(Relation from) {
    TableUnsqrlVisitor tableUnsqrlVisitor = new TableUnsqrlVisitor();
    return from.accept(tableUnsqrlVisitor, null)
        .current.get();
  }

  public class TableUnsqrlVisitor extends AstVisitor<TableRewriteScope, TableRewriteScope> {

    @Override
    public TableRewriteScope visitNode(Node node, TableRewriteScope context) {
      throw new RuntimeException("Table type not implemented to unsqrl");
    }

    @Override
    public TableRewriteScope visitTable(TableNode node, TableRewriteScope context) {
      AnalyzerTable table = getAnalyzerTable(node);
      List<Field> fieldPath = table.getFieldPath().getFields();
      Name currentAlias = null;

      Relation current;
      if (context.getCurrent().isPresent()) {
        current = context.getCurrent().get();
      } else if (fieldPath.size() == 1){
        current = node;
      } else {
        currentAlias = Name.system(gen.nextTableAlias());
        current = new TableNode(Optional.empty(), fieldPath.get(0).name.toNamePath(),
            Optional.of(currentAlias));
      }

      for (int i = 1; i < fieldPath.size(); i++) {
        Field field = fieldPath.get(i);

        Name nextAlias = (i == fieldPath.size() - 1)
            ? node.getAlias().get()
            : Name.system(gen.nextTableAlias());

        Relation lhs = current;
        Relation rhs = toTable(field, nextAlias);
        Optional<Expression> condition = getCondition(field, currentAlias, nextAlias);
        current = join(node.getLocation().get(), Type.INNER, lhs, rhs, condition);

        currentAlias = nextAlias;
      }


      Relation expanded = expandFieldPaths(table, currentAlias, current);

      return new TableRewriteScope(Optional.of(expanded));
    }

    private Relation expandFieldPaths(AnalyzerTable table, Name baseAlias, Relation current) {
      Relation toOne = expandToOne(table,baseAlias, current);
      Relation toMany = expandToMany(table, baseAlias, toOne);
      return toMany;
    }

    private Relation expandToOne(AnalyzerTable tbl, Name baseAlias, Relation current) {
      for (FieldPath path : toOneFields.get(tbl)) {
        List<Field> fields = path.getFields();

        Name tableAlias = baseAlias;

        for (int i = 0; i < fields.size() - 1; i++) {
          tableAlias = Name.system(gen.nextTableAlias());

          Field field = fields.get(i);
          Relation lhs = current;
          Relation rhs = toTable(field, tableAlias);
          Optional<Expression> condition = getCondition(field, baseAlias, tableAlias);
          current = join(null, Type.LEFT, lhs, rhs, condition);
        }

        //TODO NOT RIGHT: Should be bound to table
        for (Identifier id : toOneMapping.get(path)) {
          NamePath name = NamePath.of(tableAlias, fields.get(fields.size() - 1).getId());

          nodeMapper.put(id, new Identifier(null, name));
        }
      }

      return current;
    }

    private Relation expandToMany(AnalyzerTable tbl, Name baseAlias, Relation current) {
      for (FunctionCall call : toManyFields.get(tbl)) {

        Name tableAlias = Name.system(gen.nextTableAlias());
        Name fieldAlias = Name.system(gen.nextAlias());

        NamePath fieldName = ((Identifier)call.getArguments().get(0)).getNamePath();
        FieldPath fieldPath = tbl.getTable().getField(fieldName).get();

        fieldPath = expandToManyField(fieldPath);

        Name alias = Name.system("_");
        Relation from = toTable(new TableField(tbl.getTable()), alias);
        for (Field field : fieldPath.getFields()) {
          if (field instanceof Relationship) {
            Relationship rel = (Relationship) field;
            Name nextAlias = Name.system(gen.nextTableAlias());

            Relation lhs = from;
            Relation rhs = toTable(field, tableAlias);
            Optional<Expression> condition = getCondition(field, baseAlias, tableAlias);
            current = join(null, Type.LEFT, lhs, rhs, condition);

            from = join(null, Type.INNER, lhs, rhs, getCondition(rel,
                alias, nextAlias));
            alias = nextAlias;
          }
        }

        FunctionCall rewrittenCall = rewriteCall(call, alias, fieldPath);

        List<SelectItem> pks = new ArrayList<>();
        List<Expression> conditionList = new ArrayList<>();
        for (Column column : tbl.getTable().getPrimaryKeys()) {
          Name name = column.getName();
          Identifier identifier = new Identifier(null, NamePath.of(baseAlias, name));
          Identifier parentTable = new Identifier(null, NamePath.of(tableAlias, name));
          pks.add(new SingleColumn(identifier));
          conditionList.add(eq(identifier, parentTable));
        }

        Expression condition = and(conditionList);

        List<SelectItem> selectList = new ArrayList<>();
        selectList.addAll(pks);
        selectList.add(new SingleColumn(rewrittenCall, new Identifier(null, fieldAlias.toNamePath())));

        QuerySpecification select = new QuerySpecification(
            Optional.empty(),
            new Select(false, selectList),
            from,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());


        //Join on main table
        Relation rhs = new TableSubquery(new Query(select, Optional.empty(), Optional.empty()));
        current = join(null, Type.LEFT, current, rhs, Optional.of(condition));

        Identifier newColumn = new Identifier(null, NamePath.of(tableAlias, fieldAlias));

        nodeMapper.put(call, newColumn);
      }

      return current;
    }

    private FunctionCall rewriteCall(FunctionCall call, Name alias, FieldPath fieldPath) {
      return new FunctionCall((Optional<NodeLocation>) null,
          call.getName(),
          List.of(new Identifier(null, NamePath.of(alias, fieldPath.getLastField().getId()))),
          false,
          Optional.empty()
      );
    }

    /**
     * Checks for aggs that contain only a rel: count(rel)
     */
    private FieldPath expandToManyField(FieldPath fieldPath) {
      if (fieldPath.getLastField() instanceof Relationship) {
        Relationship rel = (Relationship) fieldPath.getLastField();
        Column pk = rel.getToTable().getPrimaryKeys().get(0);
        List<Field> fields = new ArrayList<>();
        fields.addAll(fieldPath.getFields());
        fields.add(pk);
        return new FieldPath(fields);
      }

      return fieldPath;
    }

    private Optional<Expression> getCondition(Field field, Name currentAlias, Name nextAlias) {
      if (!(field instanceof Relationship)) {
        return Optional.empty();
      }

      List<Expression> expressions = new ArrayList<>();
      Relationship rel = (Relationship) field;
      Table table = rel.getTable();
      if (rel.getType() == Relationship.Type.PARENT) {
        table = rel.getToTable();
      }

      for (Column column : table.getPrimaryKeys()) {
        Field fk = rel.getToTable().getField(column.getName());
        String lhs = rel.getPkNameMapping().get((Column) fk);
        String rhs = rel.getPkNameMapping().get(column);
        Identifier l = new Identifier(null, NamePath.parse(lhs));
        Identifier r = new Identifier(null, NamePath.parse(rhs));

        expressions.add(eq(l, r));
//        nodes.add(eq(
//            ident(currentAlias, lhs != null ? lhs : column.getId().toString()),
//            ident(nextAlias, rhs != null ? rhs : column.getId().toString())));
      }

      return Optional.ofNullable(and(expressions));
    }

    public Expression eq(Identifier l, Identifier r) {
      return new ComparisonExpression(Operator.EQUAL, l, r);
    }
    private Expression and(List<Expression> expressions) {
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

    private Relation toTable(Field field, Name nextAlias) {
      if (field instanceof SelfField) {
        return new TableNode(Optional.empty(), field.getTable().getId().toNamePath(),
            Optional.of(nextAlias));
      } else if (field instanceof Relationship) {
        if (((Relationship) field).getSqlNode() != null) {
          //return TableSubquery
          return null;//validatorProvider.create().validate(((Relationship) field).getSqlNode());
        }
//        return new SqlIdentifier(List.of(((Relationship)field).getToTable().getId().toString()), SqlParserPos.ZERO);
      } else if (field instanceof TableField) {
        return new TableNode(Optional.empty(), field.getTable().getId().toNamePath(),
            Optional.of(nextAlias));
      }
      throw new RuntimeException("Not a table:" + field);
    }

    public Join join(NodeLocation location, Type joinType, Relation left, Relation right,
        Optional<Expression> condition) {
      Optional<JoinCriteria> joinCondition = condition.map(c -> new JoinOn(location, c));

      return new Join(location, joinType, left, right, joinCondition);
    }

    @Override
    public TableRewriteScope visitJoin(Join node, TableRewriteScope context) {
      TableRewriteScope leftContext = node.getLeft().accept(this, context);

      leftContext.setPushdownCondition(node.getType(), node.getCriteria());

      TableRewriteScope rightContext = node.getRight().accept(this, leftContext);

      return rightContext;
    }

    private AnalyzerTable getAnalyzerTable(TableNode node) {
      return analyzerTableMap.get(node);
    }
  }


  @Value
  public class AnalyzerTable {
    //The table path
    FieldPath fieldPath;
    Table table;
  }

  @Value
  public class TableRewriteScope {
    Optional<Relation> current;

    public void setPushdownCondition(Type type, Optional<JoinCriteria> criteria) {

    }
  }


  private Select rewriteSelect(Select select) {
    return select;
  }
  private Optional<Expression> rewriteWhere(Optional<Expression> where) {
    return where.map( w -> ExpressionTreeRewriter.rewriteWith(new AliasRewriter(), w));
  }
  private Optional<GroupBy> rewriteGroupBy(Optional<GroupBy> groupBy) {
//    if (((SqrlValidator)validator).hasAgg(query.getSelectList())) {
//      List<SqlNode> nodes = new ArrayList<>();
//      if (group != null) {
//        nodes.addAll(group.getList());
//      }
//      List<Column> primaryKeys = this.contextTable.get().getPrimaryKeys();
//      for (int i = primaryKeys.size() - 1; i >= 0; i--) {
//        Column column = primaryKeys.get(i);
//        nodes.add(0, ident("_", column.getId().toString()));
//      }
//      return (SqlNodeList)new SqlNodeList(nodes, SqlParserPos.ZERO).accept(new ai.dataeng.sqml.parser.macros.AliasRewriter(mapper));
//    }
//
//    if (group != null) {
//      return (SqlNodeList)group.accept(new ai.dataeng.sqml.parser.macros.AliasRewriter(mapper));
//    }
//    return group;
    return groupBy;
  }
  private Optional<Expression> rewriteHaving(Optional<Expression> having) {
    return having.map( h -> ExpressionTreeRewriter.rewriteWith(new AliasRewriter(), h));
  }
  private Optional<OrderBy> rewriteOrderBy(Optional<OrderBy> orderBy) {
//    if (orderList != null && ((SqrlValidator)validator).hasAgg(query.getSelectList())) {
//      List<SqlNode> nodes = new ArrayList<>(orderList.getList());
//      //get parent primary key for context
//      List<Column> primaryKeys = contextTable.get().getPrimaryKeys();
//      for (int i = primaryKeys.size() - 1; i >= 0; i--) {
//        Column pk = primaryKeys.get(i);
//        nodes.add(0, ident("_", pk.getId().toString()));
//      }
//      return new SqlNodeList(nodes, SqlParserPos.ZERO);
//    }
//
//    return orderList;
    return orderBy;
  }
  private Optional<Limit> rewriteLimit(Optional<Limit> limit) {
    return limit;
  }
  public class AliasRewriter extends ExpressionRewriter {

  }
  @Override
  public Scope visitTable(TableNode tableNode, Scope scope) {
    NamePath tableName = tableNode.getNamePath();

    //We don't expand the table on this pass. We need to detect the to-one and to-many identifiers first
    Optional<Table> resolvedTable = analyzer.lookup(tableName);

    List<AnalyzerField> fields = createFields(resolvedTable.get(), tableNode.getAlias(), tableNode);

    return createAndAssignScope(tableNode, scope, fields);
  }

  private List<AnalyzerField> createFields(Table table, Optional<Name> alias, TableNode tableNode) {
    List<AnalyzerField> analyzerFields = new ArrayList<>();
    for (Field field : table.getFields().visibleList()) {
      AnalyzerField f = new AnalyzerField(alias, field.getName(), field.getVersion(), tableNode, field);
      analyzerFields.add(f);
    }

    return analyzerFields;
  }

  @Value
  public class AnalyzerField {
    Optional<Name> alias;
    Name name;
    int version;
    TableNode originTable;
    Field originField;
  }
  
  @Override
  public Scope visitJoin(Join node, Scope scope) {
    Scope left = node.getLeft().accept(this, scope);

    if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT || node.getCriteria().isEmpty()) {
      //Add new scope to context for a context table and alias
    }

    Scope right = node.getRight().accept(this, scope);
    List<AnalyzerField> fields = new ArrayList<>(left.getFields());
    fields.addAll(right.getFields());
    Scope result = createAndAssignScope(node, scope, fields);

    JoinCriteria criteria = node.getCriteria().get();
    if (criteria instanceof JoinOn) {
      Expression expression = ((JoinOn) criteria).getExpression();
      analyzeExpression(expression, result);
    } else {
      throw new RuntimeException("Unsupported join");
    }

    return result;
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
    //We loosen the rules of set operations and do the transfrom here.
    //We rewrite the select but place null literals in there instead (does type matter?)


//    checkState(node.getRelations().size() >= 2);
//    List<Scope> relationScopes = node.getRelations().stream()
//        .map(relation -> {
//          Scope relationScope = process(relation, scope);
//          return createAndAssignScope(relation, scope, relationScope.getRelation());
//        })
//        .collect(toImmutableList());
//
//    Type[] outputFieldTypes = relationScopes.get(0).getRelation().getFields().stream()
//        .map(Field::getType)
//        .toArray(Type[]::new);
//    int outputFieldSize = outputFieldTypes.length;
//
//    for (Scope relationScope : relationScopes) {
//      RelationType relationType = relationScope.getRelation();
//      int descFieldSize = relationType.getFields().size();
//      String setOperationName = node.getClass().getSimpleName().toUpperCase(ENGLISH);
//      if (outputFieldSize != descFieldSize) {
//        throw new RuntimeException(
//            String.format(
//            "%s query has different number of fields: %d, %d",
//            setOperationName,
//            outputFieldSize,
//            descFieldSize));
//      }
//      for (int i = 0; i < descFieldSize; i++) {
//        /*
//        Type descFieldType = relationType.getFieldByIndex(i).getType();
//        Optional<Type> commonSuperType = metadata.getTypeManager().getCommonSuperType(outputFieldTypes[i], descFieldType);
//        if (!commonSuperType.isPresent()) {
//          throw new SemanticException(
//              TYPE_MISMATCH,
//              node,
//              "column %d in %s query has incompatible types: %s, %s",
//              i + 1,
//              setOperationName,
//              outputFieldTypes[i].getDisplayName(),
//              descFieldType.getDisplayName());
//        }
//        outputFieldTypes[i] = commonSuperType.get();
//         */
//        //Super types?
//      }
//    }
//
//    TypedField[] outputDescriptorFields = new TypedField[outputFieldTypes.length];
//    RelationType firstDescriptor = relationScopes.get(0).getRelation();
//    for (int i = 0; i < outputFieldTypes.length; i++) {
//      Field oldField = (Field)firstDescriptor.getFields().get(i);
////      outputDescriptorFields[i] = new Field(
////          oldField.getRelationAlias(),
////          oldField.getName(),
////          outputFieldTypes[i],
////          oldField.isHidden(),
////          oldField.getOriginTable(),
////          oldField.getOriginColumnName(),
////          oldField.isAliased(), Optional.empty());
//    }
//
//    for (int i = 0; i < node.getRelations().size(); i++) {
//      Relation relation = node.getRelations().get(i);
//      Scope relationScope = relationScopes.get(i);
//      RelationType relationType = relationScope.getRelation();
//      for (int j = 0; j < relationType.getFields().size(); j++) {
//        Type outputFieldType = outputFieldTypes[j];
//        Type descFieldType = ((Field)relationType.getFields().get(j)).getType();
//        if (!outputFieldType.equals(descFieldType)) {
////            analysis.addRelationCoercion(relation, outputFieldTypes);
//          throw new RuntimeException(String.format("Mismatched types in set operation %s", relationType.getFields().get(j)));
////            break;
//        }
//      }
//    }
//
//    return createAndAssignScope(node, scope, new ArrayList<>(List.of(outputDescriptorFields)));
//
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
    analyzeExpression(predicate, scope);
  }


  private List<Expression> analyzeOrderBy(Optional<OrderBy> orderBy,
      Scope scope) {
    if (orderBy.isEmpty()) return List.of();
    ImmutableList.Builder<Expression> orderByFieldsBuilder = ImmutableList.builder();


    for (SortItem item : orderBy.get().getSortItems()) {
      Expression expression = item.getSortKey();
      analyzeExpression(expression, scope);
      orderByFieldsBuilder.add(expression);
    }

    List<Expression> orderByFields = orderByFieldsBuilder.build();
    return orderByFields;
  }

  private Scope createAndAssignScope(Node node, Scope parentScope, List<AnalyzerField> fields) {
    Scope scope = Scope.builder()
        .node(node)
        .fields(fields)
        .build();

    return scope;
  }

  private Scope computeAndAssignOutputScope(QuerySpecification node, Scope scope,
      Scope sourceScope) {
//    Builder<StandardField> outputFields = ImmutableList.builder();

    for (SelectItem item : node.getSelect().getSelectItems()) {
      if (item instanceof AllColumns) {
        Optional<NamePath> starPrefix = ((AllColumns) item).getPrefix();

        //Get all fields

//        for (Field field : sourceScope.resolveFieldsWithPrefix(starPrefix)) {
//          outputFields.add(new StandardField(field.getName(), field.getType(), List.of(), Optional.empty()));
//        }
      } else if (item instanceof SingleColumn) {
        SingleColumn column = (SingleColumn) item;

        Expression expression = column.getExpression();
        Optional<Identifier> field = column.getAlias();
//
//        Optional<QualifiedObjectName> originTable = Optional.empty();
//        Optional<String> originColumn = Optional.empty();
        NamePath name = null;

        if (expression instanceof Identifier) {
          name = ((Identifier) expression).getNamePath();
        }

        //Need to track the origin table (the sql-node table)
        if (name != null) {
//            List<AnalyzerField> matchingFields = sourceScope.resolveFields(name);
//            if (!matchingFields.isEmpty()) {
//              originTable = matchingFields.get(0).getOriginTable();
//              originColumn = matchingFields.get(0).getOriginColumnName();
//            }
        }

        if (field.isEmpty()) {
          if (name != null) {
//            field = Optional.of(new Identifier(getLast(name.getOriginalParts())));
          }
        }
//
//        String identifierName = field.map(Identifier::getValue)
//            .orElse("VAR");

//        outputFields.add(
//            new StandardField(Name.of(identifierName, NameCanonicalizer.SYSTEM),
//            analysis.getType(expression), List.of(), Optional.empty())
//            column.getAlias().isPresent())
//        );
      }
      else {
        throw new IllegalArgumentException("Unsupported SelectItem type: " + item.getClass().getName());
      }
    }

    return createAndAssignScope(node, scope, null);
  }

  private void analyzeHaving(QuerySpecification node, Scope scope) {
    if (node.getHaving().isPresent()) {
      Expression predicate = node.getHaving().get();
      ExpressionAnalysis expressionAnalysis = analyzeExpression(predicate, scope);
    }
  }

  private List<Expression> analyzeSelect(QuerySpecification node, Scope scope) {
    List<Expression> outputExpressions = new ArrayList<>();

    for (SelectItem item : node.getSelect().getSelectItems()) {
      if (item instanceof AllColumns) {
//        Optional<NamePath> starPrefix = ((AllColumns) item).getPrefix();
//
//        //E.g: SELECT alias.path.*
//        List<TypedField> fields = scope.resolveFieldsWithPrefix(starPrefix);
//        if (fields.isEmpty()) {
//          if (starPrefix.isPresent()) {
//            throw new RuntimeException(String.format("Table '%s' not found", starPrefix.get()));
//          }
//          throw new RuntimeException(String.format("SELECT * not allowed from relation that has no columns"));
//        }
//        for (Field field : fields) {
////            int fieldIndex = scope.getRelation().indexOf(field);
////            FieldReference expression = new FieldReference(fieldIndex);
////            outputExpressions.add(expression);
////            ExpressionAnalysis expressionAnalysis = analyzeExpression(expression, scope);
//
//          if (node.getSelect().isDistinct() && !field.getType().isComparable()) {
//            throw new RuntimeException(String.format("DISTINCT can only be applied to comparable types (actual: %s)", field.getType()));
//          }
//        }
      } else if (item instanceof SingleColumn) {
        SingleColumn column = (SingleColumn) item;
        analyzeExpression(column.getExpression(), scope);

        outputExpressions.add(column.getExpression());
      }
      else {
        throw new IllegalArgumentException(String.format("Unsupported SelectItem type: %s", item.getClass().getName()));
      }
    }

    return outputExpressions;
  }

  private List<Expression> analyzeGroupBy(QuerySpecification node, Scope scope, List<Expression> outputExpressions) {
    if (node.getGroupBy().isEmpty()) {
      return List.of();
    }

    List<Expression> groupingExpressions = new ArrayList();
    GroupingElement groupingElement = node.getGroupBy().get().getGroupingElement();
    for (Expression column : groupingElement.getExpressions()) {
      if (column instanceof LongLiteral) {
        throw new RuntimeException("Ordinals not supported in group by statements");
      }

      analyzeExpression(column, scope);

      //Group by statement must be one of the select fields
      if (!(column instanceof Identifier)) {
        log.info(
            String.format("GROUP BY statement should use column aliases instead of expressions. %s",
                column));
        analyzeExpression(column, scope);
        outputExpressions.stream()
            .filter(e -> e.equals(column))
            .findAny()
            .orElseThrow(() -> new RuntimeException(
                String.format("SELECT should contain GROUP BY expression %s", column)));
        groupingExpressions.add(column);
      }
    }

    return null;
  }

  private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
    ExpressionAnalyzer analyzer = new ExpressionAnalyzer();
    ExpressionAnalysis exprAnalysis = analyzer.analyze(expression, scope);

    return exprAnalysis;
  }
}
