package ai.datasqrl.plan.local.analyze;

import static ai.datasqrl.parse.util.SqrlNodeUtil.isExpression;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.environment.ImportManager.TableImport;
import ai.datasqrl.parse.Check;
import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.QueryAssignment;
import ai.datasqrl.parse.tree.ScriptNode;
import ai.datasqrl.parse.tree.SqrlStatement;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.local.Errors;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedTable;
import ai.datasqrl.plan.local.analyze.util.AstUtil;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.SourceTableImportMeta.RowType;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public class Analyzer extends DefaultTraversalVisitor<Scope, Scope> {

  protected final SchemaBuilder schemaBuilder;
  protected final Namespace namespace;
  private final ImportManager importManager;
  private final SchemaAdjustmentSettings schemaSettings;
  private final ErrorCollector errors;
  private final Analysis analysis;

  public Analyzer(ImportManager importManager, SchemaAdjustmentSettings schemaSettings,
      ErrorCollector errors) {
    this.importManager = importManager;
    this.schemaSettings = schemaSettings;
    this.errors = errors;
    this.analysis = new Analysis();
    this.schemaBuilder = new SchemaBuilder(analysis);
    this.namespace = new Namespace(analysis.getSchema());
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
      } else {
        throw new UnsupportedOperationException("Script imports are not yet supported");
      }
    }

    return null;
  }

  @Override
  public Scope visitDistinctAssignment(DistinctAssignment node, Scope context) {
    Check.state(node.getNamePath().getLength() == 1, node.getNamePath(),
        Errors.DISTINCT_NOT_ON_ROOT);

    Optional<ResolvedTable> tbl = namespace.resolveTable(node.getTableNode());
    Check.state(tbl.isPresent(), node.getTableNode(), Errors.TABLE_NOT_FOUND);
    Check.state(tbl.get().getPath().size() == 1, node, Errors.DISTINCT_TABLE_NESTED);

    ResolvedTable resolvedTable = tbl.get();
    Scope scope = Scope.createSingleTableScope(analysis.getSchema(), resolvedTable);
    analyzeNode(node, scope);

    List<ResolvedNamePath> pks = node.getPartitionKeyNodes().stream()
        .map(pk -> analysis.getResolvedNamePath().get(pk)).collect(Collectors.toList());
    Table distinctTable = schemaBuilder.createDistinctTable(resolvedTable, pks);

    analysis.getResolvedNamePath().put(node.getTableNode(), resolvedTable);
    analysis.getSchema().add(distinctTable);
    analysis.getProducedTable().put(node, distinctTable);

    return null;
  }

  @Override
  public Scope visitJoinAssignment(JoinAssignment node, Scope context) {
    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_TABLE);
    Check.state(node.getNamePath().getLength() > 1, node, Errors.JOIN_ON_ROOT);

    Scope scope = createLocalScope(node.getNamePath(), false);
    analyzeNode(node, scope);

    TableNode targetTableNode = AstUtil.getTargetTable(node.getJoinDeclaration());
    ResolvedNamePath resolvedTable = analysis.getResolvedNamePath().get(targetTableNode);
    Relationship relationship = schemaBuilder.addJoinDeclaration(node.getNamePath(),
        resolvedTable.getToTable(), node.getJoinDeclaration().getLimit());

    analysis.getProducedField().put(node, relationship);
    scope.getContextTable().ifPresent(t -> analysis.getParentTable().put(node, t));
    return null;
  }

  @Override
  public Scope visitExpressionAssignment(ExpressionAssignment node, Scope context) {
    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_EXPRESSION);
    Check.state(node.getNamePath().getLength() > 1, node, Errors.EXPRESSION_ON_ROOT);

    Scope scope = createLocalScope(node.getNamePath(), true);
    analyzeNode(node, scope);

    Column column = schemaBuilder.addExpression(node.getNamePath());

    //Todo: remove produced table from here
    analysis.getProducedTable().put(node, scope.getContextTable().get());
    analysis.getProducedField().put(node, column);
    scope.getContextTable().ifPresent(t -> analysis.getParentTable().put(node, t));
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
  public Scope visitQueryAssignment(QueryAssignment node, Scope context) {
    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_QUERY_TABLE);

    NamePath namePath = node.getNamePath();

    boolean isExpression = isExpression(node.getQuery());

    Scope scope = namePath.getLength() > 1 ?
        createLocalScope(namePath, isExpression) : createRootScope(namePath);
    Scope queryScope = analyzeNode(node, scope);

    if (isExpression) {
      Check.state(node.getNamePath().getLength() > 1, node, Errors.QUERY_EXPRESSION_ON_ROOT);

      analysis.getExpressionStatements().add(node);

      Column column = schemaBuilder.addQueryExpression(namePath);
      analysis.getProducedField().put(node, column);
      analysis.getProducedTable().put(node, scope.getContextTable().get());
    } else {
      Table table = schemaBuilder.addQuery(namePath, queryScope.getFieldNames());
      analysis.getProducedTable().put(node, table);
    }
    scope.getContextTable().ifPresent(t -> analysis.getParentTable().put(node, t));
    return null;
  }

  private Scope createRootScope(NamePath namePath) {
    return Scope.createScope(namePath, false, analysis.getSchema(),
        getContext(namePath.popLast()));
  }

  private Scope createLocalScope(NamePath namePath, boolean isExpression) {
    return Scope.createLocalScope(namePath, isExpression, analysis.getSchema(),
        getContext(namePath.popLast()));
  }

  private Scope analyzeNode(Node node, Scope scope) {
    NodeAnalyzer analyzer = new NodeAnalyzer(errors, analysis, namespace);
    return node.accept(analyzer, scope);
  }

  private Optional<Table> getContext(NamePath namePath) {
    if (namePath.getLength() == 0) {
      return Optional.empty();
    }
    Table table = analysis.getSchema().getVisibleByName(namePath.getFirst()).get();
    return table.walkTable(namePath.popFirst());
  }

  private boolean canAssign(NamePath namePath) {
    return true;
  }
}
