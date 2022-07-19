package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import lombok.Getter;

@Getter
public class Relationship extends Field {

  private final Table fromTable;
  private final Table toTable;
  private final JoinType joinType;
  private final Multiplicity multiplicity;

  public Relationship(Name name, Table fromTable, Table toTable, JoinType joinType,
                      Multiplicity multiplicity) {
    super(name);
    this.fromTable = fromTable;
    this.toTable = toTable;
    this.joinType = joinType;
    this.multiplicity = multiplicity;
  }

  @Override
  public String toString() {
    return fromTable.getId() + " -> " + toTable.getId()
            + " [" + joinType + "," + multiplicity + "]";
  }

  public enum JoinType {
    PARENT, CHILD, JOIN
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY
  }
}