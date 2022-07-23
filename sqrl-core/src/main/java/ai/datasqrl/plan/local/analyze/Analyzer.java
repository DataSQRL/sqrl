package ai.datasqrl.plan.local.analyze;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.environment.ImportManager.TableImport;
import ai.datasqrl.parse.Check;
import ai.datasqrl.parse.tree.*;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.sqrl.table.CalciteTableFactory;
import ai.datasqrl.plan.local.Errors;
import ai.datasqrl.plan.local.ImportedTable;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedTable;
import ai.datasqrl.plan.local.analyze.util.AstUtil;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.ScriptTable;
import ai.datasqrl.schema.input.SchemaAdjustmentSettings;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Triple;

import java.util.List;
import java.util.Optional;

import static ai.datasqrl.parse.util.SqrlNodeUtil.isExpression;

@Getter
public class Analyzer extends DefaultTraversalVisitor<Scope, Scope> {

  protected final CalciteTableFactory tableFactory;
  protected final VariableFactory variableFactory;
  protected final Namespace namespace;
  private final ImportManager importManager;
  private final SchemaAdjustmentSettings schemaSettings;
  private final ErrorCollector errors;
  private final Analysis analysis;

  public Analyzer(ImportManager importManager, SchemaAdjustmentSettings schemaSettings,
                  CalciteTableFactory tableFactory, ErrorCollector errors) {
    this.importManager = importManager;
    this.schemaSettings = schemaSettings;
    this.errors = errors;
    this.tableFactory = tableFactory;
    this.analysis = new Analysis();
    this.variableFactory = new VariableFactory();
    this.namespace = new Namespace();
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
    if (node.getNamePath().size() != 2) {
      throw new RuntimeException(
          String.format("Invalid import identifier: %s", node.getNamePath()));
    }

    //Check if this imports all or a single table
    Name sourceDataset = node.getNamePath().get(0);
    Name sourceTable = node.getNamePath().get(1);

    List<TableImport> importTables;
    Optional<Name> nameAlias = node.getAliasName();

    if (sourceTable.equals(ReservedName.ALL)) { //import all tables from dataset
      importTables = importManager.importAllTables(sourceDataset, schemaSettings, errors);
      nameAlias = Optional.empty();
      throw new RuntimeException("TBD");
    } else { //importing a single table
      //todo: Check if table exists
      TableImport tblImport = importManager.importTable(sourceDataset, sourceTable,
          schemaSettings, errors);
      if (!tblImport.isSource()) {
        throw new RuntimeException("TBD");
      }
      SourceTableImport sourceTableImport = (SourceTableImport)tblImport;

      ImportedTable importedTable = tableFactory.importTable(sourceTableImport, nameAlias);
      namespace.scopeDataset(importedTable, nameAlias);
      analysis.getImportDataset().put(node, List.of(importedTable));
    }

    return null;
  }

  @Override
  public Scope visitDistinctAssignment(DistinctAssignment node, Scope context) {
    Check.state(node.getNamePath().size() == 1, node.getNamePath(),
        Errors.DISTINCT_NOT_ON_ROOT);

    Optional<ResolvedTable> tbl = namespace.resolveTable(node.getTableNode());
    Check.state(tbl.isPresent(), node.getTableNode(), Errors.TABLE_NOT_FOUND);
    Check.state(tbl.get().getPath().size() == 1, node, Errors.DISTINCT_TABLE_NESTED);

    ResolvedTable resolvedTable = tbl.get();
    Scope scope = Scope.createSingleTableScope(namespace, resolvedTable);
    analyzeNode(node, scope);

    ScriptTable distinctTable = variableFactory.createDistinctTable(resolvedTable);
    namespace.addTable(distinctTable);

    analysis.getProducedFieldList().put(node, distinctTable.getFields().toList());
    analysis.getResolvedNamePath().put(node.getTableNode(), resolvedTable);
    analysis.getProducedTable().put(node, distinctTable);

    return null;
  }

  @Override
  public Scope visitJoinAssignment(JoinAssignment node, Scope context) {
    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_TABLE);
    Check.state(node.getNamePath().size() > 1, node, Errors.JOIN_ON_ROOT);

    Scope scope = createLocalScope(node.getNamePath(), false);
    analyzeNode(node, scope);

    TableNode targetTableNode = AstUtil.getTargetTable(node.getJoinDeclaration());
    ResolvedNamePath resolvedTable = analysis.getResolvedNamePath().get(targetTableNode);
    ScriptTable parentTable = scope.getContextTable().get();
    Relationship relationship = variableFactory.addJoinDeclaration(node.getNamePath(), parentTable,
        resolvedTable.getToTable(), node.getJoinDeclaration().getLimit());

    analysis.getProducedField().put(node, relationship);
    scope.getContextTable().ifPresent(t -> analysis.getParentTable().put(node, t));
    return null;
  }

  @Override
  public Scope visitExpressionAssignment(ExpressionAssignment node, Scope context) {
    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_EXPRESSION);
    Check.state(node.getNamePath().size() > 1, node, Errors.EXPRESSION_ON_ROOT);

    Scope scope = createLocalScope(node.getNamePath(), true);
    analyzeNode(node, scope);
    ScriptTable table = scope.getContextTable().get();

    Column column = variableFactory.addExpression(node.getNamePath(), table);

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

    Scope scope = namePath.size() > 1 ?
        createLocalScope(namePath, isExpression) : createRootScope(namePath);
    Scope queryScope = analyzeNode(node, scope);

    if (isExpression) {
      Check.state(node.getNamePath().size() > 1, node, Errors.QUERY_EXPRESSION_ON_ROOT);

      analysis.getExpressionStatements().add(node);
      ScriptTable table = scope.getContextTable().get();

      Column column = variableFactory.addQueryExpression(namePath, table);
      analysis.getProducedField().put(node, column);
      analysis.getProducedTable().put(node, scope.getContextTable().get());
      analysis.getProducedFieldList().put(node, List.of(column));
    } else {
      Triple<Optional<Relationship>, ScriptTable, List<Field>> table = variableFactory.addQuery(
          namePath, queryScope.getFieldNames(), scope.getContextTable());
      analysis.getProducedFieldList().put(node, table.getMiddle().getFields().toList());
      analysis.getProducedTable().put(node, table.getMiddle());
      table.getLeft().ifPresent(r -> analysis.getProducedField().put(node, r));
      if (namePath.size() == 1) {
        namespace.addTable(table.getMiddle());
      }
    }
    scope.getContextTable().ifPresent(t -> analysis.getParentTable().put(node, t));

    return null;
  }

  private Scope createRootScope(NamePath namePath) {
    return Scope.createScope(namePath, false, namespace,
        getContext(namePath.popLast()));
  }

  private Scope createLocalScope(NamePath namePath, boolean isExpression) {
    return Scope.createLocalScope(namePath, isExpression, namespace,
        Optional.of(getContextOrThrow(namePath.popLast())));
  }

  private ScriptTable getContextOrThrow(NamePath namePath) {
    return getContext(namePath).orElseThrow(()->new RuntimeException(""));
  }

  private Scope analyzeNode(Node node, Scope scope) {
    NodeAnalyzer analyzer = new NodeAnalyzer(errors, analysis, namespace);
    return node.accept(analyzer, scope);
  }

  private Optional<ScriptTable> getContext(NamePath namePath) {
    if (namePath.size() == 0) {
      return Optional.empty();
    }
    return namespace.getTable(namePath);
  }

  private boolean canAssign(NamePath namePath) {
    return true;
  }
}
