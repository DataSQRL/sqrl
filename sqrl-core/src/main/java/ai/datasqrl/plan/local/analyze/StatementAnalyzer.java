package ai.datasqrl.plan.local.analyze;

import static ai.datasqrl.parse.util.SqrlNodeUtil.isExpression;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.environment.ImportManager.SourceTableImport;
import ai.datasqrl.environment.ImportManager.TableImport;
import ai.datasqrl.parse.Check;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.JoinAssignment;
import ai.datasqrl.parse.tree.QueryAssignment;
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

public class StatementAnalyzer extends Analyzer {

  public StatementAnalyzer(ImportManager importManager,
      SchemaAdjustmentSettings schemaSettings,
      ErrorCollector errors) {
    super(importManager, schemaSettings, errors);
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
    Check.state(tbl.get().getPath().size() > 1, node, Errors.DISTINCT_TABLE_NESTED);

    ResolvedTable resolvedTable = tbl.get();
    Scope scope = createSingleTableScope(resolvedTable);
    super.visitDistinctAssignment(node, scope);

    List<ResolvedNamePath> pks = node.getPartitionKeyNodes().stream()
        .map(pk -> analysis.getResolvedNamePath().get(pk))
        .collect(Collectors.toList());
    Table distinctTable = schemaBuilder.createDistinctTable(resolvedTable, pks);

    analysis.getResolvedNamePath().put(node.getTableNode(), resolvedTable);
    analysis.getSchema().add(distinctTable);
    analysis.getProducedTable().put(node, distinctTable);

    return null;
  }

  @Override
  public Scope visitJoinAssignment(JoinAssignment node, Scope context) {
    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_TABLE);

    Scope scope = createSelfScope(node.getNamePath(), false);
    super.visitJoinAssignment(node, scope);

    TableNode targetTableNode = AstUtil.getTargetTable(node.getJoinDeclaration());
    ResolvedNamePath resolvedTable = analysis.getResolvedNamePath().get(targetTableNode);
    Relationship relationship = schemaBuilder.addJoinDeclaration(node.getNamePath(),
        resolvedTable.getToTable(),
        node.getJoinDeclaration().getLimit());

    analysis.getProducedField().put(node, relationship);

    return null;
  }

  @Override
  public Scope visitExpressionAssignment(ExpressionAssignment node, Scope context) {
    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_EXPRESSION);

    Scope scope = createSelfScope(node.getNamePath(), true);
    super.visitExpressionAssignment(node, scope);

    Column column = schemaBuilder.addExpression(node.getNamePath());

    analysis.getProducedTable()
        .put(node, scope.getContextTable().get()); //not produced but still needed todo:fix
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
  public Scope visitQueryAssignment(QueryAssignment node, Scope context) {
    Check.state(canAssign(node.getNamePath()), node, Errors.UNASSIGNABLE_QUERY_TABLE);

    NamePath namePath = node.getNamePath();

    boolean isExpression = isExpression(node.getQuery());

    Scope scope = createScope(namePath, isExpression);
    Scope queryScope = super.visitQueryAssignment(node, scope);

    if (isExpression) {
      analysis.getExpressionStatements().add(node);

      Column column = schemaBuilder.addQueryExpression(namePath);
      analysis.getProducedField().put(node, column);
      analysis.getProducedTable().put(node, scope.getContextTable().get());
    } else {
      Table table = schemaBuilder.addQuery(namePath, queryScope.getFieldNames());
      analysis.getProducedTable().put(node, table);
    }

    return null;
  }

  private boolean canAssign(NamePath namePath) {
    return true;
  }
}
