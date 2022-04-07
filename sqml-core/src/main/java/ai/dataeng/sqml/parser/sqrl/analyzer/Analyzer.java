package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Relationship.Multiplicity;
import ai.dataeng.sqml.parser.Relationship.Type;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.operator.ImportManager;
import ai.dataeng.sqml.parser.sqrl.LogicalDag;
import ai.dataeng.sqml.parser.sqrl.SqrlRexUtil;
import ai.dataeng.sqml.parser.sqrl.calcite.CalcitePlanner;
import ai.dataeng.sqml.parser.sqrl.schema.SqrlViewTable;
import ai.dataeng.sqml.parser.sqrl.schema.StreamTable.StreamDataType;
import ai.dataeng.sqml.parser.sqrl.schema.TableFactory;
import ai.dataeng.sqml.parser.sqrl.transformers.ExpressionToQueryTransformer;
import ai.dataeng.sqml.tree.AllColumns;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.JoinDeclaration;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqrlRelBuilder;
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
      analyzeQuery(queryAssignment.getNamePath(), queryAssignment.getQuery());
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
      analyzeQuery(namePath, query);

      return null;
    }

    public void analyzeQuery(NamePath namePath, Query query) {
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(analyzer);

      //For single unnamed columns in tables, treat as an expression
//      if (hasOneUnnamedColumn(query) && namePath.getPrefix().isPresent()) {
      if (true) {
        Optional<Table> tableOpt = lookup(namePath.getPrefix().get());
        Table table = tableOpt.get();
        Scope scope = query.accept(statementAnalyzer, new Scope(tableOpt, query, new HashMap<>()));

        System.out.println(NodeFormatter.accept(scope.getNode()));

        SqlNode plan = planner.parse(scope.getNode());
        System.out.println(plan);
        System.out.println();

        //todo after sqlizing
        if (false) {
          RelNode plan2 = planner.plan(null);

          RelNode expanded = plan2.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
              StreamDataType dataType = (StreamDataType) scan.getRowType();
              if (dataType.getTable() != null) {
                return dataType.getTable().getRelNode();
              }
              return scan;
            }
          });

          SqrlRelBuilder relBuilder = planner.createRelBuilder();
          RexBuilder rexBuilder = relBuilder.getRexBuilder();

          RelNode newPlan = relBuilder
              .push(expanded)
              .push(table.getRelNode())
              .join(JoinRelType.LEFT,
                  SqrlRexUtil.createPkCondition(table, rexBuilder))
              //todo: project all left + the new column, shadow if necessary
              .build();

          table.setRelNode(newPlan);
        }

        int version = 0;
        Optional<Field> existingField = table.getFields().getByName(namePath.getLast());
        if (existingField.isPresent()) {
          version = existingField.get().getVersion() + 1;
        }
        table.addField(new Column(namePath.getLast(), table, version, null, 0, List.of(),
            false, false, Optional.empty(), false));
      } else {
       throw new RuntimeException("");
      }
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
      Table table = tableFactory.create(node.getNamePath(), node.getTable());
//      Scope scope = new Scope(Optional.empty(), node, new LinkedHashMap<>());
      Optional<Table> oldTable = lookup(node.getTable().toNamePath());

      // https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/deduplication/

      SqlParser parser = SqlParser.create("SELECT _uuid, _ingest_time, customerid, email, name\n"
          + "FROM (\n"
          + "   SELECT _uuid, _ingest_time, customerid, email, name,\n"
          + "     ROW_NUMBER() OVER (PARTITION BY customerid\n"
          + "       ORDER BY _ingest_time DESC) AS rownum\n"
          + "   FROM "+oldTable.get().getId().toString()+")\n"
          + "WHERE rownum = 1");

      SqlNode sqlNode = parser.parseQuery();
      SqlValidator validator = planner.getValidator();
      validator.validate(sqlNode);
      SqlToRelConverter sqlToRelConverter = planner.getSqlToRelConverter(validator);
      RelNode relNode = sqlToRelConverter.convertQuery(sqlNode, false, true).rel;

      for (Field field : oldTable.get().getFields()) {
        if (field instanceof Column) {
          Column f = new Column(field.getName(), table, field.getVersion(), null, 0, List.of(), false, false, Optional.empty(), false);
          if (node.getPartitionKeys().contains(field.getName())) {
            f.setPrimaryKey(true);
          }
          table.addField(f);
        }
      }

      RelNode expanded = relNode.accept(new RelShuttleImpl(){
        @Override
        public RelNode visit(TableScan scan) {
          return oldTable.get().getRelNode();
        }
      });

      table.setRelNode(expanded);
      StreamDataType streamDataType = new StreamDataType(table, expanded.getRowType().getFieldList());

      planner.getSchema().add(table.getId().toString(), new SqrlViewTable(streamDataType));

      logicalDag.getSchema().add(table);

      return null;
    }

    @Override
    public Void visitJoinDeclaration(JoinDeclaration node, Void context) {
      NamePath namePath = node.getNamePath();

      StatementAnalyzer sAnalyzer = new StatementAnalyzer(analyzer);

      Select select = new Select(Optional.empty(), false, List.of(new AllColumns()));
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

      System.out.println(NodeFormatter.accept(querySpec));

      Optional<Table> ctxTable = lookup(namePath.getPrefix().get());
      Scope scope = querySpec.accept(sAnalyzer, new Scope(ctxTable, querySpec, new HashMap<>()));

      Map<Name, Table> joinScope = scope.getJoinScope();
      Node rewritten = scope.getNode();
      if (node.getInlineJoin().getLimit().isEmpty()) {
//        rewritten = (Join)((QuerySpecification)((Query)rewritten).getQueryBody()).getFrom();
      }

      Table table = ctxTable.get();
      Map.Entry<Name, Table> lastTable = joinScope.entrySet().stream().findFirst().get();
      Multiplicity multiplicity = Multiplicity.MANY;
      if (node.getInlineJoin().getLimit().isPresent() && node.getInlineJoin().getLimit().get().getIntValue().get() == 1) {
        multiplicity = Multiplicity.ONE;
      }

      Relationship joinField = new Relationship(namePath.getLast(), table, lastTable.getValue(),
          Type.JOIN, multiplicity, null);
      joinField.setNode(rewritten);
      joinField.setAlias(lastTable.getKey());

      table.addField(joinField);

      return null;
    }
  }

  public Optional<Table> lookup(NamePath namePath) {
    //Get w/context in path
//    if (namePath.getFirst().equals(Name.SELF_IDENTIFIER)) {
//      Table table = scope.getContextTable().get();
//      if (namePath.getLength() > 1) {
//        table = table.walk(namePath.popFirst())
//            .get();
//      }
//      return Optional.of(table);
//    } else {
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
