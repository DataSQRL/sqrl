//package ai.datasqrl.plan;
//
//import ai.datasqrl.SqrlSchema;
//import ai.datasqrl.parse.tree.name.NamePath;
//import ai.datasqrl.plan.local.ImportedTable;
//import ai.datasqrl.plan.local.generate.QueryGenerator.FieldNames;
//import ai.datasqrl.schema.ScriptTable;
//import java.util.Optional;
//import lombok.Getter;
//import org.apache.calcite.jdbc.CalciteSchema;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.schema.SchemaPlus;
//import org.apache.calcite.schema.Table;
//import org.apache.calcite.sql.JoinDeclaration;
//import org.apache.calcite.sql.JoinDeclarationContainerImpl;
//import org.apache.calcite.sql.TableMapperImpl;
//
///**
// * Maps a user visible calcite schema with the optimizing schema
// */
//@Getter
//public class SchemaMapping {
//  final FieldNames fieldNames = new FieldNames();
//  TableMapperImpl tableMapper;
//
//  private final JoinDeclarationFactory joinDeclarationFactory;
//  JoinDeclarationContainerImpl joinDecs;
//
//  public void importTable(ImportedTable d) {
//      //Add all Calcite tables to the schema
////      this.tables.put(d.getImpTable().getNameId(), d.getImpTable());
//      d.getShredTableMap().values().stream().forEach(vt -> shredSchema.add(vt.getNameId(),vt));
//
//      //Update table mapping from SQRL table to Calcite table...
////      this.tableMap.putAll(d.getShredTableMap());
//      //and also map all fields
//      this.tableMapper.getTableMap().putAll(d.getShredTableMap());
//
//      this.fieldNames.putAll(d.getFieldNameMap());
//
//      d.getShredTableMap().keySet().stream().flatMap(t->t.getAllRelationships())
//          .forEach(r->{
//            JoinDeclaration dec = createParentChildJoinDeclaration(r, tableMapper, uniqueAliasGenerator);
//            joinDecs.add(r, dec);
//          });
//
//      SqrlSchema datasets = new SqrlSchema();
//      transpileSchema.add(d.getName().getDisplay(), datasets);
//      transpileSchema.add(d.getTable().getName().getDisplay(),
//          (Table)d.getTable());
//
//  }
//
//  public void importSchema(ImportSchema schema) {
//
//  }
//
//  public void addVisibleTable(Table visibleTable) {
//
//  }
//
//  public void addQuery(RelNode relNode, Optional<ScriptTable> table, NamePath path) {
//
//    //todo:
////    SqlNode sqlNode = generateDistinctQuery(node);
////    //Field names are the same...
////    ScriptTable table = analysis.getProducedTable().get(node);
////
////    RelNode relNode = plan(sqlNode);
////    for (int i = 0; i < analysis.getProducedFieldList().get(node).size(); i++) {
////      this.fieldNames.put(analysis.getProducedFieldList().get(node).get(i),
////          relNode.getRowType().getFieldList().get(i).getName());
////    }
////
////    int numPKs = node.getPartitionKeyNodes().size();
////    QueryCalciteTable queryTable = new QueryCalciteTable(relNode);
////    this.tables.put(queryTable.getNameId(), queryTable);
////    this.tableMap.put(table, queryTable);
////    return sqlNode;
//  }
//}
