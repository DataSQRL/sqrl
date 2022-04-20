package ai.datasqrl.transform;

import static ai.datasqrl.parse.util.SqrlNodeUtil.hasOneUnnamedColumn;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeFormatter;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.sql.calcite.NodeToSqlNodeConverter;
import ai.datasqrl.transform.transforms.AliasFirstColumn;
import ai.datasqrl.transform.transforms.ExpressionToQueryTransformer;
import ai.datasqrl.transform.transforms.DistinctToSqlNode;
import ai.datasqrl.validate.scopes.DistinctScope;
import ai.datasqrl.validate.scopes.StatementScope;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;

@Slf4j
@AllArgsConstructor
public class StatementTransformer {
  protected final ErrorCollector errors = ErrorCollector.root();

  public SqlNode transform(Node statement, StatementScope statementScope) {
    Visitor visitor = new Visitor();
    return statement.accept(visitor, statementScope);
  }

  public class Visitor extends AstVisitor<SqlNode, StatementScope> {
    /**
     * Noop
     */
    @Override
    public SqlNode visitImportDefinition(ImportDefinition node, StatementScope scope) {
      return null;
    }

    @Override
    public SqlNode visitQueryAssignment(QueryAssignment queryAssignment, StatementScope scope) {
      transformStatement(queryAssignment.getNamePath(), queryAssignment.getQuery(), scope);
      return null;
    }

    @Override
    public SqlNode visitExpressionAssignment(ExpressionAssignment assignment, StatementScope scope) {
      NamePath namePath = assignment.getNamePath();
      Expression expression = assignment.getExpression();

      ExpressionToQueryTransformer expressionToQueryTransformer =
          new ExpressionToQueryTransformer(scope.getContextTable().get());
      Query query = expressionToQueryTransformer.transform(expression);
      scope.getScopes().putAll(expressionToQueryTransformer.getScopes());

      return transformStatement(namePath, query, scope);
    }

    public SqlNode transformStatement(NamePath namePath, Query query, StatementScope scope) {
      log.info("Sqrl Query: {}", NodeFormatter.accept(query));
      if (hasOneUnnamedColumn(query)) {
        Preconditions.checkState(namePath.getPrefix().isPresent());
        return analyzeExpression(namePath, query, scope);
      } else {
        return null;//analyzeQuery(namePath, query);
      }
    }

    /**
     * Analyzes a query as an expression but has some handling to account for shadowing.
     */
    public SqlNode analyzeExpression(NamePath namePath, Query query,
        StatementScope scope) {
      Query aliasedQuery = new AliasFirstColumn().transform(query, namePath.getLast());
      QueryTransformer queryTransformer = new QueryTransformer();
      Node node = aliasedQuery.accept(queryTransformer, scope);

      NodeToSqlNodeConverter converter = new NodeToSqlNodeConverter();
      SqlNode sqlNode = node.accept(converter, null);

      return sqlNode;
    }

//    //We are creating a new table but analyzing it in a similar way
//    private Node analyzeQuery(NamePath namePath, Query query) {
//      //Could be a base table
//      Optional<Table> scopeTable = namePath.getPrefix().flatMap(p-> getTable(p));
//
//      QueryTransformer queryValidator = new QueryTransformer();
//      Scope scope = query.accept(queryValidator, null);
//
//      SqlNode sqlNode = planner.parse(scope.getNode());
//      log.info("Calcite Query: {}", sqlNode);
//
//      RelNode plan = planner.plan(sqlNode);
//      RelNode expanded = plan.accept(viewExpander);
//      //Revalidate expanded
//      planner.getValidator().validate(RelToSql.convertToSqlNode(expanded));
//
//      Table newTable = new Table(SourceTablePlanner.tableIdCounter.incrementAndGet(), namePath.getLast(),
//          namePath, false);
//      newTable.setRelNode(expanded);
//
//      //Append columns
//      List<Field> columns = createFieldsFromSelect(newTable, (Query)scope.getNode(), plan);
//      columns.forEach(newTable::addField);
//
//      //Update schema
//      ViewFactory viewFactory = new ViewFactory();
//      planner.getSchema().add(newTable.getId().toString(), viewFactory.create(expanded));
//
//      if (scopeTable.isPresent()) {
//        Table ctxTable = scopeTable.get();
//
//        Relationship parent = new Relationship(Name.PARENT_RELATIONSHIP, newTable, ctxTable,
//            Type.PARENT, Multiplicity.ONE);
//        newTable.addField(parent);
//
//        Relationship relationship = new Relationship(namePath.getLast(), ctxTable, newTable,
//            Type.CHILD, Multiplicity.MANY);
//        ctxTable.addField(relationship);
//      } else {
//        schema.add(newTable);
//      }
//      return null;
//    }
//
//    private List<Field> createFieldsFromSelect(Table table, Query node, RelNode plan) {
//      return new TableFactory().create(node)
//          .getFields().getElements();
////
////      List<Column> columns = new ArrayList<>();
////      if (node.getQueryBody() instanceof QuerySpecification) {
////        QuerySpecification spec = (QuerySpecification) node.getQueryBody();
////        for (SelectItem selectItem : spec.getSelect().getSelectItems()) {
////          SingleColumn singleColumn = (SingleColumn) selectItem;
////          Name name = singleColumn.getAlias().map(i->i.getNamePath().getLast())
////              .orElseGet(()->((Identifier) singleColumn.getExpression()).getNamePath().getLast());
////          Column column = table.fieldFactory(name);
////          RelDataTypeField relField = plan.getRowType().getField(name.getCanonical(), false, false);
////          column.setType(RelDataTypeConverter.toBasicType(relField.getType()));
////          columns.add(column);
////        }
////
////        return columns;
////      }
////
////      throw new RuntimeException("not implemented yet");
//    }
//
//    /**
//     * Checks if an expression has an AS function as its column
//     */
//    private boolean hasNamedColumn(Expression expression) {
//      if(expression instanceof FunctionCall) {
//        return ((FunctionCall) expression).getNamePath().equals(Name.system("AS").toNamePath());
//      }
//      return false;
//    }
//
//    @Override
//    public Node visitCreateSubscription(CreateSubscription subscription, StatementScope scope) {
//      return null;
//    }

    @SneakyThrows
    @Override
    public SqlNode visitDistinctAssignment(DistinctAssignment node, StatementScope scope) {
      DistinctScope distinctScope = (DistinctScope)scope.getScopes().get(node);

      DistinctToSqlNode transform = new DistinctToSqlNode();
      return transform.transform(node, distinctScope);
    }
//
//    @Override
//    public Node visitJoinDeclaration(JoinDeclaration node, StatementScope scope) {
//      NamePath namePath = node.getNamePath();
//
//      Name name = getLastTableName(node);
//      QueryTransformer queryValidator = new QueryTransformer();
//
//      Select select = new Select(Optional.empty(), false, List.of(new AllColumns(name.toNamePath())));
//      Query querySpec = new Query(new QuerySpecification(node.getLocation(),
//          select,
//          node.getInlineJoin().getRelation(),
//          Optional.<Expression>empty(),
//          Optional.<GroupBy>empty(),
//          Optional.<Expression>empty(),
//          node.getInlineJoin().getOrderBy(),
//          node.getInlineJoin().getLimit()),
//          Optional.empty(),
//          Optional.empty()
//      );
//
//      Optional<Table> ctxTable = getTable(namePath.getPrefix().get());
//      Scope scope2 = querySpec.accept(
//          queryValidator, null);
//
//      Node rewritten = scope2.getNode();
//
//      Table table = ctxTable.get();
//
//      //TODO: fix me
//      List<Map.Entry<Name, Table>> list = new ArrayList(scope2.getJoinScope().entrySet());
//
//      Table lastTable = list.get(list.size() - 1).getValue();
//      Multiplicity multiplicity = Multiplicity.MANY;
//      if (node.getInlineJoin().getLimit().isPresent() && node.getInlineJoin().getLimit().get().getIntValue().get() == 1) {
//        multiplicity = Multiplicity.ONE;
//      }
//
//      Relationship joinField = new Relationship(namePath.getLast(), table, lastTable,
//          Type.JOIN, multiplicity);
//      joinField.setNode(rewritten);
//
//      joinField.setAlias(list.get(list.size() - 1).getKey());
//      table.addField(joinField);
//
//      return null;
//    }
//
//    private Name getLastTableName(JoinDeclaration node) {
//      Relation rel = node.getInlineJoin().getRelation();
//      while (rel instanceof Join) {
//        rel = ((Join) rel).getRight();
//      }
//      TableNode table = (TableNode) rel;
//
//      return table.getAlias().orElse(table.getNamePath().getFirst());
//    }
//  }
//
//  public Optional<Table> getTable(NamePath namePath) {
//      Optional<Table> schemaTable = this.schema.getByName(namePath.getFirst());
//      if (schemaTable.isPresent()) {
//        if (namePath.getLength() == 1) {
//          return schemaTable;
//        }
//
//        return schemaTable.flatMap(t-> t.walk(namePath.popFirst()));
//      }
//      return Optional.empty();
////    }
      @Override
      public SqlNode visitNode(Node node, StatementScope scope) {
        throw new RuntimeException(String.format("Could not process node %s : %s", node.getClass().getName(), node));
      }

  }
}
