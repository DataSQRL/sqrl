package ai.dataeng.sqml.parser.sqrl.analyzer;

import static ai.dataeng.sqml.util.SqrlNodeUtil.hasOneUnnamedColumn;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.RelDataTypeConverter;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Relationship.Multiplicity;
import ai.dataeng.sqml.parser.Relationship.Type;
import ai.dataeng.sqml.parser.SqrlQueries;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ImportManager;
import ai.dataeng.sqml.parser.sqrl.LogicalDag;
import ai.dataeng.sqml.parser.sqrl.schema.SqrlViewTable;
import ai.dataeng.sqml.parser.sqrl.schema.StreamTable.StreamDataType;
import ai.dataeng.sqml.parser.sqrl.schema.TableFactory;
import ai.dataeng.sqml.parser.sqrl.transformers.ExpressionToQueryTransformer;
import ai.dataeng.sqml.parser.sqrl.transformers.Transformers;
import ai.dataeng.sqml.planner.CalcitePlanner;
import ai.dataeng.sqml.planner.DagExpander;
import ai.dataeng.sqml.planner.RelToSql;
import ai.dataeng.sqml.planner.ViewFactory;
import ai.dataeng.sqml.tree.AllColumns;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.JoinDeclaration;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;

@Slf4j
@AllArgsConstructor
public class Analyzer {
  private ImportManager importManager;
  private CalcitePlanner planner;
  private TableFactory tableFactory;
  private LogicalDag logicalDag;
  private DagExpander dagExpander;

  //Keep namespace here
  protected final ErrorCollector errors = ErrorCollector.root();

  public Analysis analyze(ScriptNode script) {
    Visitor visitor = new Visitor(this);
    script.accept(visitor, null);
    return new Analysis(logicalDag);
  }

  @Value
  public static class Analysis {
    LogicalDag dag;
  }

  public class Visitor extends AstVisitor<Void, Void> {
    private final AtomicBoolean importResolved = new AtomicBoolean(false);
    private final Analyzer analyzer;

    public Visitor(Analyzer analyzer) {
      this.analyzer = analyzer;
    }

    @Override
    public Void visitNode(Node node, Void context) {
      throw new RuntimeException(String.format("Could not process node %s : %s", node.getClass().getName(), node));
    }

    @Override
    public Void visitScript(ScriptNode node, Void context) {
      List<Node> statements = node.getStatements();
      for (int i = 0; i < statements.size(); i++) {
        statements.get(i).accept(this, null);

        //Test for end of imports
        Optional<Node> nextStatement = (i < statements.size()) ?
          Optional.of(statements.get(i)) : Optional.empty();
        if (nextStatement.map(s->!(s instanceof ImportDefinition))
            .orElse(false)) {
          importResolved.set(true);
        }
      }

      return null;
    }

    //TODO:
    // import ds.*;
    // import ns.function;
    // import ds;
    @Override
    public Void visitImportDefinition(ImportDefinition node, Void context) {
      if (importResolved.get()) {
        throw new RuntimeException(String.format("Import statement must be in header %s", node.getNamePath()));
      }

      if (node.getNamePath().getLength() > 2) {
        throw new RuntimeException(String.format("Cannot import identifier: %s", node.getNamePath()));
      }

      importManager.setTableFactory(new TableFactory(planner));
      Table table = importManager.resolveTable(node.getNamePath().get(0), node.getNamePath().get(1),
          node.getAliasName(), errors);
      logicalDag.getSchema().add(table);
      return null;
    }

    @Override
    public Void visitQueryAssignment(QueryAssignment queryAssignment, Void context) {
      analyzeStatement(queryAssignment.getNamePath(), queryAssignment.getQuery());
      return null;
    }

    @Override
    public Void visitExpressionAssignment(ExpressionAssignment assignment, Void context) {
      NamePath namePath = assignment.getNamePath();
      Expression expression = assignment.getExpression();

      Optional<Table> table = logicalDag.getSchema().getByName(namePath.getFirst());
      Preconditions.checkState(table.isPresent(), "Expression cannot be assigned to root");
      Preconditions.checkState(!hasNamedColumn(expression), "Expressions cannot be named");

      Query query = createExpressionQuery(expression);
      analyzeStatement(namePath, query);

      return null;
    }

    public void analyzeStatement(NamePath namePath, Query query) {
      log.info("Sqrl Query: {}", NodeFormatter.accept(query));
      if (hasOneUnnamedColumn(query)) {
        Preconditions.checkState(namePath.getPrefix().isPresent());
        analyzeExpression(namePath, query);
      } else {
        analyzeQuery(namePath, query);
      }
    }

    /**
     * Analyzes a query as an expression but has some handling to account for shadowing.
     */
    public void analyzeExpression(NamePath namePath, Query query) {
      Table contextTable = namePath.getPrefix().flatMap(p-> getTable(p)).get();
      Query aliasedQuery = Transformers.aliasFirstColumn.transform(query, namePath.getLast());
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analyzer);
      Scope scope = aliasedQuery.accept(statementAnalyzer, new Scope(Optional.of(contextTable), aliasedQuery, new HashMap<>(),new HashMap<>(),
          true, namePath.getLast()));

      SqlNode sqlNode = planner.parse(scope.getNode());
      log.info("Calcite Query: {}", sqlNode);

      RelNode plan = planner.plan(sqlNode);

      /*
       * Add columns to schema.
       *
       * Associate the derived type with the column. SQRL types are not used in the query
       * analysis, but they are used at graphql query time.
       */
      Column column = contextTable.fieldFactory(namePath.getLast());
      RelDataTypeField field = plan.getRowType().getField(column.getId().toString(), false, false);
      column.setType(RelDataTypeConverter.toBasicType(field.getType()));
      contextTable.addField(column);

      /*
       * Expand dag
       */
      RelNode expanded = plan.accept(dagExpander);
      //Revalidate expanded
      planner.getValidator().validate(RelToSql.convertToSqlNode(expanded));

      contextTable.setRelNode(expanded);

      //Update the calcite schema so new columns are visible
      ViewFactory viewFactory = new ViewFactory();
      planner.setView(contextTable.getId().toString(), viewFactory.create(expanded));
    }

    //We are creating a new table but analyzing it in a similar way
    private void analyzeQuery(NamePath namePath, Query query) {
      //Could be a base table
      Optional<Table> contextTable = namePath.getPrefix().flatMap(p-> getTable(p));

      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analyzer);
      Scope scope = query.accept(statementAnalyzer, new Scope(contextTable, query, new HashMap<>(),new HashMap<>(),
          false, namePath.getLast()));

      SqlNode sqlNode = planner.parse(scope.getNode());
      log.info("Calcite Query: {}", sqlNode);

      RelNode plan = planner.plan(sqlNode);
      RelNode expanded = plan.accept(dagExpander);
      //Revalidate expanded
      planner.getValidator().validate(RelToSql.convertToSqlNode(expanded));

      Table newTable = new Table(TableFactory.tableIdCounter.incrementAndGet(), namePath.getLast(),
          namePath, false);
      newTable.setRelNode(expanded);

      //Append columns
      List<Field> columns = createFieldsFromSelect(newTable, (Query)scope.getNode(), plan);
      columns.forEach(newTable::addField);

      //Update schema
      ViewFactory viewFactory = new ViewFactory();
      planner.getSchema().add(newTable.getId().toString(), viewFactory.create(expanded));

      if (contextTable.isPresent()) {
        Table ctxTable = contextTable.get();

        Relationship parent = new Relationship(Name.PARENT_RELATIONSHIP, newTable, ctxTable,
            Type.PARENT, Multiplicity.ONE);
        newTable.addField(parent);

        Relationship relationship = new Relationship(namePath.getLast(), ctxTable, newTable,
            Type.CHILD, Multiplicity.MANY);
        ctxTable.addField(relationship);
      } else {
        logicalDag.getSchema().add(newTable);
      }

    }

    private List<Field> createFieldsFromSelect(Table table, Query node, RelNode plan) {
      return new ai.dataeng.sqml.parser.TableFactory().create(new TableSubquery(node))
          .getFields().getElements();
//
//      List<Column> columns = new ArrayList<>();
//      if (node.getQueryBody() instanceof QuerySpecification) {
//        QuerySpecification spec = (QuerySpecification) node.getQueryBody();
//        for (SelectItem selectItem : spec.getSelect().getSelectItems()) {
//          SingleColumn singleColumn = (SingleColumn) selectItem;
//          Name name = singleColumn.getAlias().map(i->i.getNamePath().getLast())
//              .orElseGet(()->((Identifier) singleColumn.getExpression()).getNamePath().getLast());
//          Column column = table.fieldFactory(name);
//          RelDataTypeField relField = plan.getRowType().getField(name.getCanonical(), false, false);
//          column.setType(RelDataTypeConverter.toBasicType(relField.getType()));
//          columns.add(column);
//        }
//
//        return columns;
//      }
//
//      throw new RuntimeException("not implemented yet");
    }

    /**
     * Checks if an expression has an AS function as its column
     */
    private boolean hasNamedColumn(Expression expression) {
      if(expression instanceof FunctionCall) {
        return ((FunctionCall) expression).getName().equals(Name.system("AS").toNamePath());
      }
      return false;
    }

    private Query createExpressionQuery(Expression expression) {
      ExpressionToQueryTransformer expressionToQueryTransformer = new ExpressionToQueryTransformer();
      return expressionToQueryTransformer.transform(expression);
    }

    @Override
    public Void visitCreateSubscription(CreateSubscription subscription, Void context) {
      return null;
    }

    @SneakyThrows
    @Override
    public Void visitDistinctAssignment(DistinctAssignment node, Void context) {
      Optional<Table> refTable = getTable(node.getTable().toNamePath());

      Table table = tableFactory.create(node.getNamePath(), node.getTable());
      List<Column> fields = refTable.get().getFields().visibleList().stream().filter(f->f instanceof Column)
          .map(f->(Column)f).collect(
          Collectors.toList());
      // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/deduplication/
      String sql = SqrlQueries.generateDistinct(node,
          refTable.get(),
          node.getPartitionKeys().stream()
              .map(name->refTable.get().getField(name).getId().toString())
              .collect(Collectors.toList()),
          fields.stream().map(e->e.getId().toString()).collect(Collectors.toList())
          );
      SqlParser parser = SqlParser.create(sql);

      SqlNode sqlNode = parser.parseQuery();
      SqlValidator validator = planner.getValidator();
      validator.validate(sqlNode);
      SqlToRelConverter sqlToRelConverter = planner.getSqlToRelConverter(validator);
      RelNode relNode = sqlToRelConverter.convertQuery(sqlNode, false, true).rel;

      for (Field field : fields) {
        if (field instanceof Column) {
          Column f = new Column(field.getName(), table, field.getVersion(),
              null, 0, List.of(), false, false, Optional.empty(), false);
          if (node.getPartitionKeys().contains(field.getName())) {
            f.setPrimaryKey(true);
          }
          table.addField(f);
        }
      }

      RelNode expanded = relNode.accept(new RelShuttleImpl(){
        @Override
        public RelNode visit(TableScan scan) {
          return refTable.get().getRelNode();
        }
      });
      table.setRelNode(expanded);

      StreamDataType streamDataType = new StreamDataType(table, expanded.getRowType().getFieldList());

      planner.getSchema().add(table.getId().toString(), new SqrlViewTable(streamDataType, relNode));

      logicalDag.getSchema().add(table);

      return null;
    }

    @Override
    public Void visitJoinDeclaration(JoinDeclaration node, Void context) {
      NamePath namePath = node.getNamePath();

      Name name = getLastTableName(node);
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analyzer);

      Select select = new Select(Optional.empty(), false, List.of(new AllColumns(name.toNamePath())));
      Query querySpec = new Query(new QuerySpecification(node.getLocation(),
          select,
          node.getInlineJoin().getRelation(),
          Optional.<Expression>empty(),
          Optional.<GroupBy>empty(),
          Optional.<Expression>empty(),
          node.getInlineJoin().getOrderBy(),
          node.getInlineJoin().getLimit()),
          Optional.empty(),
          Optional.empty()
      );

      Optional<Table> ctxTable = getTable(namePath.getPrefix().get());
      Scope scope = querySpec.accept(statementAnalyzer, new Scope(ctxTable, querySpec, new LinkedHashMap<>(),new LinkedHashMap<>(),
          false, null));

      Node rewritten = scope.getNode();

      Table table = ctxTable.get();

      //TODO: fix me
      List<Map.Entry<Name, Table>> list = new ArrayList(scope.getJoinScope().entrySet());

      Table lastTable = list.get(list.size() - 1).getValue();
      Multiplicity multiplicity = Multiplicity.MANY;
      if (node.getInlineJoin().getLimit().isPresent() && node.getInlineJoin().getLimit().get().getIntValue().get() == 1) {
        multiplicity = Multiplicity.ONE;
      }

      Relationship joinField = new Relationship(namePath.getLast(), table, lastTable,
          Type.JOIN, multiplicity);
      joinField.setNode(rewritten);

      joinField.setAlias(list.get(list.size() - 1).getKey());
      table.addField(joinField);

      return null;
    }

    private Name getLastTableName(JoinDeclaration node) {
      Relation rel = node.getInlineJoin().getRelation();
      while (rel instanceof Join) {
        rel = ((Join) rel).getRight();
      }
      TableNode table = (TableNode) rel;

      return table.getAlias().orElse(table.getNamePath().getFirst());
    }
  }

  public Optional<Table> getTable(NamePath namePath) {
      Optional<Table> schemaTable = this.logicalDag.getSchema().getByName(namePath.getFirst());
      if (schemaTable.isPresent()) {
        if (namePath.getLength() == 1) {
          return schemaTable;
        }

        return schemaTable.flatMap(t-> t.walk(namePath.popFirst()));
      }
      return Optional.empty();
//    }
  }
  public LogicalDag getDag() {
    return this.logicalDag;
  }
}
