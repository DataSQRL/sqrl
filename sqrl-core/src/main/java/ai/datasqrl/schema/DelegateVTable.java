package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.ReservedName;
import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.VirtualSqrlTable;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.ArraySqlType;

@Getter
public class DelegateVTable extends VarTable {

  private final VirtualSqrlTable vtable;
//  private final List<RelDataTypeField> relFields;

  public DelegateVTable(Name tableName, VirtualSqrlTable vtable) {
    super(tableName.toNamePath());
    this.vtable = vtable;
//    this.relFields = fields;
    unpack(vtable.getQueryRowType().getFieldList());
  }

  private void unpack(List<RelDataTypeField> fields) {
    for (RelDataTypeField field : fields) {
      if (vtable.getChildren().containsKey(field.getName())) {
        DelegateVTable toTable = new DelegateVTable(Name.system(field.getName()),
            vtable.getChildren().get(field.getName()));

        DelegateRelRelationship rel = new DelegateRelRelationship(field, Name.system(field.getName()), 0,
            this, toTable, JoinType.CHILD, field.getType() instanceof RelRecordType ? Multiplicity.ONE :
            Multiplicity.MANY);
        this.fields.addField(rel);

        Relationship parent = new Relationship(ReservedName.PARENT, 0,
            toTable, this, JoinType.PARENT, Multiplicity.ONE);
        toTable.fields.addField(parent);
      } else {
        this.fields.addField(new DelegateRelColumn(Name.system(field.getName()), 0));
      }
    }
  }

  public Map<String, AbstractSqrlTable> getShredTables() {
    Map<String, AbstractSqrlTable> map = new HashMap<>();
    for (Field field : fields.toList()) {
      if (field instanceof DelegateRelRelationship) {
        DelegateRelRelationship rel = (DelegateRelRelationship) field;
        DelegateVTable toTable = (DelegateVTable)rel.getToTable();
        map.put(toTable.getVtable().getNameId(), toTable.getVtable());
      }
    }
    return map;
  }

  public Map<VarTable, AbstractSqrlTable> getShredTableMap() {
    Map<VarTable, AbstractSqrlTable> map = new HashMap<>();
    for (Field field : fields.toList()) {
      if (field instanceof DelegateRelRelationship) {
        DelegateRelRelationship rel = (DelegateRelRelationship) field;
        DelegateVTable toTable = (DelegateVTable)rel.getToTable();
        map.put(toTable, toTable.getVtable());
      }
    }
    return map;
  }
}
