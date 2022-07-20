package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataTypeField;

@Getter
public class DelegateRelRelationship extends Relationship {

  private final RelDataTypeField relField;

  public DelegateRelRelationship(RelDataTypeField relField, Name name, int version, VarTable fromTable, VarTable toTable,
      JoinType joinType, Multiplicity multiplicity) {
    super(name, version, fromTable, toTable, joinType, multiplicity);
    this.relField = relField;
  }
}
