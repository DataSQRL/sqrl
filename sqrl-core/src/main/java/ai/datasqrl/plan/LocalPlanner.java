package ai.datasqrl.plan;

import static ai.datasqrl.parse.tree.name.Name.INGEST_TIME;
import static ai.datasqrl.plan.util.FlinkSchemaUtil.getIndex;
import static ai.datasqrl.plan.util.FlinkSchemaUtil.requiresShredding;
import static ai.datasqrl.schema.util.KeyRemapper.remapParentPrimary;
import static ai.datasqrl.schema.util.KeyRemapper.remapPrimary;

import ai.datasqrl.execute.flink.ingest.schema.FlinkTableConverter;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.ExpressionAssignment;
import ai.datasqrl.parse.tree.ImportDefinition;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.LocalPlanner.PlannerScope;
import ai.datasqrl.plan.calcite.CalcitePlanner;
import ai.datasqrl.plan.shred.ShredPlanner;
import ai.datasqrl.plan.util.FlinkRelDataTypeConverter;
import ai.datasqrl.plan.util.FlinkSchemaUtil;
import ai.datasqrl.plan.util.ViewExpander;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.operations.AddColumnOp;
import ai.datasqrl.schema.operations.AddDatasetOp;
import ai.datasqrl.schema.operations.AddQueryOp;
import ai.datasqrl.schema.operations.ImportTable;
import ai.datasqrl.schema.operations.SchemaOperation;
import ai.datasqrl.validate.scopes.DistinctScope;
import ai.datasqrl.validate.scopes.ImportScope;
import ai.datasqrl.validate.scopes.StatementScope;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Schema;

public class LocalPlanner extends AstVisitor<SchemaOperation, PlannerScope> {

  private final FlinkTableConverter tbConverter = new FlinkTableConverter();
  private final CalcitePlanner calcitePlanner;

  public LocalPlanner(ai.datasqrl.schema.Schema schema) {
    calcitePlanner = new CalcitePlanner(schema);
  }

  public SchemaOperation plan(Node node, SqlNode sqlNode,
      StatementScope scope) {
    return node.accept(this, createScope(scope, sqlNode));
  }

  private PlannerScope createScope(StatementScope scope, SqlNode sqlNode) {
    return new PlannerScope(scope, sqlNode);
  }

  @Value
  public class PlannerScope {

    StatementScope scope;
    SqlNode sqlNode;
  }

  /**
   * Import
   */
  @Override
  public SchemaOperation visitImportDefinition(ImportDefinition node, PlannerScope scope) {
    ImportScope importScope = (ImportScope) scope.getScope().getScopes().get(node);

    //Walk each discovered table, add NamePath Map
    Pair<Schema, TypeInformation> tbl = tbConverter
        .tableSchemaConversion(importScope.getSourceTableImport().getSourceSchema());
    Schema schema = tbl.getKey();

    //Import statements can be aliased
    Name tableName = node.getAliasName().orElse(importScope.getSourceTableImport().getTableName());

    List<ImportTable> importedPaths = new ArrayList<>();

    RelDataType streamRelType = FlinkRelDataTypeConverter.toRelDataType(schema.getColumns());
    RelNode relNode = calcitePlanner.createRelBuilder()
        .scanStream(tableName, streamRelType)
        .watermark(getIndex(tbl.getLeft(), INGEST_TIME.getCanonical()))
        .project(FlinkRelDataTypeConverter.getScalarIndexes(schema))
        .build();

    ImportTable table = new ImportTable(tableName.toNamePath(),
        relNode,
        FlinkSchemaUtil.getFieldNames(relNode),
        Set.of(getIndex(relNode.getRowType(), "_uuid")),
        Set.of());

    importedPaths.add(table);

    if (requiresShredding(schema)) {
      ShredPlanner shredPlanner = new ShredPlanner(this.calcitePlanner);
      importedPaths.addAll(shredPlanner.shred(tableName, streamRelType, schema));
    }

    return new AddDatasetOp(importedPaths);
  }

  @Override
  public SchemaOperation visitDistinctAssignment(DistinctAssignment node, PlannerScope context) {
    SqlValidator validator = calcitePlanner.getValidator();
    SqlNode validated = validator.validate(context.getSqlNode());
    SqlToRelConverter sqlToRelConverter = calcitePlanner.getSqlToRelConverter(validator);

    RelNode relNode = sqlToRelConverter.convertQuery(validated, false, true).rel;
    DistinctScope distinctScope = (DistinctScope) context.getScope().getScopes().get(node);

    RelNode expanded = relNode.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        return distinctScope.getTable().getRelNode();
      }
    });

    Set<Integer> primaryKeys = new HashSet<>();
    for (Field field : distinctScope.getPartitionKeys()) {
      int index = getIndex(expanded.getRowType(), field.getName().getCanonical());
      primaryKeys.add(index);
    }

    List<Name> fields = distinctScope.getTable()
        .getFields().stream()
        .filter(f -> f instanceof Column)
        .map(Field::getName)
        .collect(Collectors.toList());

    return new AddQueryOp(node.getNamePath(), expanded, fields, primaryKeys, Set.of());
  }

  @Override
  public SchemaOperation visitExpressionAssignment(ExpressionAssignment node,
      PlannerScope context) {
    RelNode plan = this.calcitePlanner.plan(context.getSqlNode());
    Table contextTable = context.getScope().getContextTable().get();
    //The field has not been added to the table yet, infer its name
    Name fieldName = contextTable.getNextFieldName(node.getNamePath().getLast());

    Integer index = getIndex(plan.getRowType(), fieldName.getCanonical());

    RelNode expanded = plan.accept(new ViewExpander(this.calcitePlanner));

    Set<Integer> primaryKeys = remapPrimary(contextTable, expanded);
    Set<Integer> parentPrimary = remapParentPrimary(contextTable, expanded);

    return new AddColumnOp(node.getNamePath().popLast(), node.getNamePath().getLast(), expanded,
        index, primaryKeys,
        parentPrimary);
  }
}
