package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import lombok.Getter;

@Getter
public class Relationship extends Field {

  private final VarTable fromTable;
  private final VarTable toTable;
  private final JoinType joinType;
  private final Multiplicity multiplicity;

  public Relationship(Name name, int version, VarTable fromTable, VarTable toTable, JoinType joinType,
                      Multiplicity multiplicity) {
    super(name, version);
    this.fromTable = fromTable;
    this.toTable = toTable;
    this.joinType = joinType;
    this.multiplicity = multiplicity;
  }

  @Override
  public String toString() {
    return fromTable.getName() + " -> " + toTable.getName()
            + " [" + joinType + "," + multiplicity + "]";
  }

  public enum JoinType {
    PARENT, CHILD, JOIN
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY
  }
}