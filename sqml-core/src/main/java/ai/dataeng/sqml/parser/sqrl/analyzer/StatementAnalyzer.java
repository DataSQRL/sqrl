package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.sqrl.analyzer.ExpressionAnalyzer.ExpressionAnalysis;
import ai.dataeng.sqml.tree.AllColumns;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Except;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.GroupingElement;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Intersect;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.JoinCriteria;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SetOperation;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatementAnalyzer extends AstVisitor<Scope, Scope> {

  private final Analyzer analyzer;

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

    return createAndAssignScope(node, scope, (List<Field>)queryBodyScope.getFields());
  }

    @Override
  public Scope visitQuerySpecification(QuerySpecification node, Scope scope) {
    Scope sourceScope = node.getFrom().accept(this, scope);

    node.getWhere().ifPresent(where -> analyzeWhere(node, sourceScope, where));
    List<Expression> outputExpressions = analyzeSelect(node, sourceScope);
    List<Expression> groupByExpressions = analyzeGroupBy(node, sourceScope, outputExpressions);
    analyzeHaving(node, sourceScope);
//    Scope outputScope = computeAndAssignOutputScope(node, scope, sourceScope);
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
//
    Optional<Long> multiplicity = node.parseLimit();
//
    List<Expression> sourceExpressions = new ArrayList<>(outputExpressions);
    node.getHaving().ifPresent(sourceExpressions::add);
//
//    analyzeGroupingOperations(node, sourceExpressions, orderByExpressions);
//    List<FunctionCall> aggregates = analyzeAggregations(node, sourceExpressions, orderByExpressions);
//
//    if (!aggregates.isEmpty() && groupByExpressions.isEmpty()) {
//      // Have Aggregation functions but no explicit GROUP BY clause
////      analysis.setGroupByExpressions(node, ImmutableList.of());
//    }
//
//    verifyAggregations(node, sourceScope, orderByScope, groupByExpressions, sourceExpressions, orderByExpressions);


//    transformDistinct(query);
//    scope = new SqrlTranslator.Scope(query);
//    this.aggs.putAll(analyzeToManyAggs(query));
//    analyzeToOneFields(query);
//
//    SqrlTranslator.Scope fromScope = visitFrom(query.getFrom(), scope);
//    query.setFrom(fromScope.node);
//
//    query.setSelectList(rewriteSelectItems(query, fromScope));
//
//    query.setWhere(rewriteWhere(query.getWhere(), fromScope));
//    query.setGroupBy(rewriteGroupBy(query.getGroup(), query, fromScope));
//    query.setHaving(rewriteHaving(query.getHaving(), fromScope));
//    query.setOrderBy(rewriteOrderBy(query.getOrderList(), query, fromScope));
//    query.setFetch(rewriteFetch(query.getFetch(), fromScope));
//    query.setOffset(rewriteOffset(query.getOffset(), fromScope));



    return null;
  }

  @Override
  public Scope visitTable(TableNode tableNode, Scope scope) {
    NamePath tableName = tableNode.getNamePath();

    //We don't expand the table on this pass. We need to detect the to-one and to-many identifiers first
    Optional<Table> resolvedTable = analyzer.lookup(tableName);

    //The table that gets resolved will need to be distinct so we can match up expanded identifiers

    //Map aliases to fields
    tableNode.getAlias();
    resolvedTable.get().getFields().getElements();

    return createAndAssignScope(tableNode, scope, null);
  }


  @Override
  public Scope visitJoin(Join node, Scope scope) {
    Scope left = node.getLeft().accept(this, scope);

    if (node.getType() == Join.Type.CROSS || node.getType() == Join.Type.IMPLICIT || node.getCriteria().isEmpty()) {
      //Add new scope to context for a context table and alias
    }

    Scope right = node.getRight().accept(this, scope);
    List<Field> fields = new ArrayList<>(left.getFields());
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
  public Scope visitSetOperation(SetOperation node, Scope scope)
  {
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

  private Scope createAndAssignScope(Node node, Scope parentScope, List<Field> fields) {
//    Scope scope = Scope.builder()
//        .withParent(parentScope)
//        .withRelationType(relationType)
//        .build();

//    analysis.setScope(node, scope);

    return null;
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
//            List<Field> matchingFields = sourceScope.resolveFields(name);
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
