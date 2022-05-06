//package ai.datasqrl.plan.local;
//
//import static ai.datasqrl.parse.tree.name.Name.INGEST_TIME;
//import static ai.datasqrl.plan.util.FlinkSchemaUtil.getIndex;
//import static ai.datasqrl.plan.util.FlinkSchemaUtil.requiresShredding;
//
//import ai.datasqrl.execute.flink.ingest.schema.FlinkTableConverter;
//import ai.datasqrl.parse.tree.AstVisitor;
//import ai.datasqrl.parse.tree.DistinctAssignment;
//import ai.datasqrl.parse.tree.ExpressionAssignment;
//import ai.datasqrl.parse.tree.ImportDefinition;
//import ai.datasqrl.parse.tree.Join;
//import ai.datasqrl.parse.tree.JoinDeclaration;
//import ai.datasqrl.parse.tree.Node;
//import ai.datasqrl.parse.tree.QueryAssignment;
//import ai.datasqrl.parse.tree.Relation;
//import ai.datasqrl.parse.tree.SingleColumn;
//import ai.datasqrl.parse.tree.TableNode;
//import ai.datasqrl.parse.tree.name.Name;
//import ai.datasqrl.parse.tree.name.NamePath;
//import ai.datasqrl.plan.calcite.CalcitePlanner;
//import ai.datasqrl.plan.local.LocalPlanner.PlannerScope;
//import ai.datasqrl.plan.local.operations.AddColumnOp;
//import ai.datasqrl.plan.local.operations.AddDatasetOp;
//import ai.datasqrl.plan.local.operations.AddQueryOp;
//import ai.datasqrl.plan.local.operations.AddRelationshipOp;
//import ai.datasqrl.plan.local.operations.ImportTable;
//import ai.datasqrl.plan.local.operations.SchemaUpdateOp;
//import ai.datasqrl.plan.local.shred.ShredPlanner;
//import ai.datasqrl.plan.util.FlinkRelDataTypeConverter;
//import ai.datasqrl.plan.util.FlinkSchemaUtil;
//import ai.datasqrl.plan.util.ViewExpander;
//import ai.datasqrl.schema.Column;
//import ai.datasqrl.schema.Field;
//import ai.datasqrl.schema.Relationship.Multiplicity;
//import ai.datasqrl.schema.Table;
//import ai.datasqrl.validate.scopes.DistinctScope;
//import ai.datasqrl.validate.scopes.ImportScope;
//import ai.datasqrl.validate.scopes.InlineJoinScope;
//import ai.datasqrl.validate.scopes.QueryScope;
//import ai.datasqrl.validate.scopes.QuerySpecScope;
//import ai.datasqrl.validate.scopes.StatementScope;
//import ai.datasqrl.validateSql.SqlResult;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Optional;
//import java.util.Set;
//import java.util.stream.Collectors;
//import lombok.Value;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.RelShuttleImpl;
//import org.apache.calcite.rel.core.TableScan;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.sql2rel.SqlToRelConverter;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.table.api.Schema;
//
//@Slf4j
//public class LocalPlanner extends AstVisitor<SchemaUpdateOp, PlannerScope> {
//
//  private final FlinkTableConverter tbConverter = new FlinkTableConverter();
//  private final CalcitePlanner calcitePlanner;
//
//  public LocalPlanner(ai.datasqrl.schema.Schema schema) {
//    calcitePlanner = new CalcitePlanner(schema);
//  }
//
//  public SchemaUpdateOp plan(Node node, Optional<Node> transformed,
//      StatementScope scope, Optional<SqlResult> sqlResult) {
//    return node.accept(this, createScope(scope, transformed, sqlResult));
//  }
//
//  private PlannerScope createScope(StatementScope scope, Optional<Node> node, Optional<SqlResult> sqlResult) {
//    return new PlannerScope(scope, node, sqlResult);
//  }
//
//  @Value
//  public class PlannerScope {
//    StatementScope scope;
//    Optional<Node> node;
//    Optional<SqlResult> sqlResult;
//  }
//
//  /**
//   * Import
//   */
//  @Override
//  public SchemaUpdateOp visitImportDefinition(ImportDefinition node, PlannerScope scope) {
//    ImportScope importScope = (ImportScope) scope.getScope().getScopes().get(node);
//
//    //Walk each discovered table, add NamePath Map
//    Pair<Schema, TypeInformation> tbl = tbConverter
//        .tableSchemaConversion(importScope.getSourceTableImport().getSourceSchema());
//    Schema schema = tbl.getKey();
//
//    //Import statements can be aliased
//    Name tableName = node.getAliasName().orElse(importScope.getSourceTableImport().getTableName());
//
//    List<ImportTable> importedPaths = new ArrayList<>();
//
//    RelDataType streamRelType = FlinkRelDataTypeConverter.toRelDataType(schema.getColumns());
//    RelNode relNode = calcitePlanner.createRelBuilder()
//        .scanStream(tableName, streamRelType)
//        .watermark(getIndex(tbl.getLeft(), INGEST_TIME.getCanonical()))
//        .project(FlinkRelDataTypeConverter.getScalarIndexes(schema))
//        .build();
//    log.info("Plan {}:", relNode.explain());
//
//    ImportTable table = new ImportTable(tableName.toNamePath(),
////        relNode,
//        FlinkSchemaUtil.getFieldNames(relNode),
//        Set.of(getIndex(relNode.getRowType(), "_uuid")),
//        Set.of());
//
//    importedPaths.add(table);
//
//    if (requiresShredding(schema)) {
//      ShredPlanner shredPlanner = new ShredPlanner(this.calcitePlanner);
//      importedPaths.addAll(shredPlanner.shred(tableName, streamRelType, schema));
//    }
//
//    return new AddDatasetOp(importedPaths);
//  }
//
//  @Override
//  public SchemaUpdateOp visitDistinctAssignment(DistinctAssignment node, PlannerScope context) {
//    SqlToRelConverter sqlToRelConverter = calcitePlanner.getSqlToRelConverter(context.getSqlResult().get().getSqlValidator());
//
//    RelNode relNode = sqlToRelConverter.convertQuery(context.getSqlResult().get().getSqlNode(), false, true).rel;
//    log.info("Plan {}:", relNode.explain());
//
//    DistinctScope distinctScope = (DistinctScope) context.getScope().getScopes().get(node);
//
//    RelNode expanded = relNode.accept(new RelShuttleImpl() {
//      @Override
//      public RelNode visit(TableScan scan) {
//        return distinctScope.getTable().getRelNode();
//      }
//    });
//
//    Set<Integer> primaryKeys = new HashSet<>();
//    for (Field field : distinctScope.getPartitionKeys()) {
//      int index = getIndex(expanded.getRowType(), field.getName().getCanonical());
//      primaryKeys.add(index);
//    }
//
//    List<Name> fields = distinctScope.getTable()
//        .getFields().stream()
//        .filter(f -> f instanceof Column)
//        .map(Field::getName)
//        .collect(Collectors.toList());
//
//    return new AddQueryOp(node.getNamePath(),  fields, primaryKeys, List.of());
//  }
//
//  @Override
//  public SchemaUpdateOp visitExpressionAssignment(ExpressionAssignment node,
//      PlannerScope context) {
//    return expression(node.getNamePath(), context);
//  }
//
//  public SchemaUpdateOp expression(NamePath namePath, PlannerScope context){
//    RelNode plan = this.calcitePlanner.plan(context.getSqlResult().get().getSqlNode(), context.getSqlResult().get().getSqlValidator());
//    log.info("Plan {}:", plan.explain());
//
//    Table contextTable = context.getScope().getContextTable().get();
//    //The field has not been added to the table yet, infer its name
//    Name fieldName = contextTable.getNextFieldId(namePath.getLast());
//
//    Integer index = getIndex(plan.getRowType(), fieldName.getCanonical());
//
//    RelNode expanded = plan.accept(new ViewExpander(this.calcitePlanner));
//
//    return new AddColumnOp(namePath.popLast(), namePath.getLast()
////        expanded,
////        index, Set.of(),
////        Set.of()
//    );
//  }
//
//  @Override
//  public SchemaUpdateOp visitJoinDeclaration(JoinDeclaration node, PlannerScope context) {
//    InlineJoinScope scope = (InlineJoinScope)context.getScope().getScopes().get(node);
//
//    RelNode plan = this.calcitePlanner.plan(context.getSqlResult().get().getSqlNode(), context.getSqlResult().get().getSqlValidator());
//    log.info("Plan {}:", plan.explain());
//
//    Multiplicity multiplicity = node.getInlineJoin().getLimit()
//        .flatMap(l->l.getIntValue())
//        .map(i->i == 1 ? Multiplicity.ONE : Multiplicity.MANY)
//        .orElse(Multiplicity.MANY);
////    return new AddRelationshipOp(node.getNamePath(), scope.getToTable(), multiplicity, node.getInlineJoin(),
////        getLastTableName(node));
//    return null;
//  }
//
//  private Name getLastTableName(JoinDeclaration node) {
//    Relation rel = node.getInlineJoin().getRelation();
//    while (rel instanceof Join) {
//      rel = ((Join) rel).getRight();
//    }
//    TableNode table = (TableNode) rel;
//
//    return table.getAlias().orElse(Name.system(table.getNamePath().toString()));
//  }
//
//  @Override
//  public SchemaUpdateOp visitQueryAssignment(QueryAssignment node, PlannerScope context) {
//    if (context.getScope().getScopes().get(node.getQuery()) instanceof QueryScope) {
//      QueryScope queryScope = (QueryScope)context.getScope().getScopes().get(node.getQuery());
//      if (queryScope.isExpression()) {
//        return expression(node.getNamePath(), context);
//      }
//    }
//
//    RelNode plan = this.calcitePlanner.plan(context.getSqlResult().get().getSqlNode(),
//        context.getSqlResult().get().getSqlValidator());
//    log.info("Plan {}:", plan.explain());
//
//    Table contextTable = context.getScope().getContextTable().get();
//
//    RelNode expanded = plan.accept(new ViewExpander(this.calcitePlanner));
//
////    Set<Integer> primaryKeys = remapPrimary(contextTable, expanded);
////    Set<Integer> parentPrimary = remapParentPrimary(contextTable, expanded);
//    QuerySpecScope querySpecScope = (QuerySpecScope)context.getScope().getScopes().get(node.getQuery().getQueryBody());
//
//    List<Name> fields = querySpecScope.getSelect().stream()
//        .map(s->((SingleColumn)s).getAlias().get().getNamePath().getFirst())
//        .collect(Collectors.toList());
//    return new AddQueryOp(node.getNamePath(),  fields, Set.of(), List.of());
//  }
//}
