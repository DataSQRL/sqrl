package ai.datasqrl.plan;

import ai.datasqrl.execute.flink.ingest.schema.FlinkTableConverter;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.CalcitePlanner;
import ai.datasqrl.plan.nodes.StreamTable.StreamDataType;
import ai.datasqrl.validate.scopes.ImportScope;
import ai.datasqrl.validate.scopes.StatementScope;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.types.DataType;

public class LocalPlanner extends AstVisitor<LocalPlannerResult, StatementScope> {

  public LocalPlannerResult plan(Node sqlNode, StatementScope scope) {
    return sqlNode.accept(this, scope);
  }

  @Override
  public LocalPlannerResult visitImportDefinition(ImportDefinition node, StatementScope scope) {
    CalcitePlanner calcitePlanner = new CalcitePlanner();

    ImportScope importScope = (ImportScope) scope.getScopes().get(node);

    //Walk each discovered table, add NamePath Map
    List<ImportTable> importedPaths = new ArrayList<>();

    FlinkTableConverter tbConverter = new FlinkTableConverter();
    Pair<Schema, TypeInformation> tbl = tbConverter
        .tableSchemaConversion(importScope.getSourceTableImport().getSourceSchema());
    Schema schema = tbl.getKey();
    TypeInformation typeInfo = tbl.getValue();
    RelOptCluster cluster = calcitePlanner.getCluster();
    RelOptSchema schema1 = calcitePlanner.getCatalogReader();

    Name name = node.getAliasName().orElse(importScope.getSourceTableImport().getTableName());

    RelOptTable table = RelOptTableImpl.create(schema1, toRelDataType(schema.getColumns()),
        List.of(name.getCanonical()), null);
    LogicalTableScan logicalTableScan = new LogicalTableScan(cluster, RelTraitSet.createEmpty(), List.of(),
        table);
    importedPaths.add(new ImportTable(name.toNamePath(), logicalTableScan,
        schema.getColumns().stream().map(e->Name.system(e.getName())).collect(
        Collectors.toList())));

    return new ImportLocalPlannerResult(importedPaths);
  }

  private RelDataType toRelDataType(List<UnresolvedColumn> c) {
    List<UnresolvedPhysicalColumn> columns = c.stream()
        .map(column->(UnresolvedPhysicalColumn) column)
        .collect(Collectors.toList());

    FlinkTypeFactory factory = new FlinkTypeFactory(new FlinkTypeSystem());

    List<RelDataTypeField> fields = new ArrayList<>();
    for (UnresolvedPhysicalColumn column : columns) {
      fields.add(new RelDataTypeFieldImpl(column.getName(), fields.size(),
          factory.createFieldTypeFromLogicalType(((DataType)column.getDataType()).getLogicalType())));
    }

    return new StreamDataType(null, fields);
  }
}
