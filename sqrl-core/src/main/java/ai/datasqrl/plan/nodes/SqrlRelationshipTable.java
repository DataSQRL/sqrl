//package ai.datasqrl.plan.nodes;
//
//import ai.datasqrl.schema.Relationship;
//import lombok.Getter;
//import org.apache.calcite.rel.RelNode;
//import org.apache.calcite.rel.type.RelDataType;
//import org.apache.calcite.rel.type.RelDataTypeFactory;
//import org.apache.calcite.schema.impl.AbstractTable;
//
//@Getter
//public class SqrlRelationshipTable extends AbstractTable {
//
//  RelNode relNode;
//  RelDataType dataType;
//
//  public SqrlRelationshipTable(Relationship rel) {
//    //todo: add from table pk (aliased?)
//    this.dataType = rel.getToTable().getHead().getRowType();
//    this.relNode = rel.getToTable().getHead();
//  }
//
//  @Override
//  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
//    return this.dataType;
//  }
//}
