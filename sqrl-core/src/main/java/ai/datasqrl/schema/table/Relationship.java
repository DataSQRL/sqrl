package ai.datasqrl.schema.table;

import ai.datasqrl.parse.tree.name.Name;
import lombok.Getter;

@Getter
public class Relationship<V extends VirtualTable> extends Field {

  private final TableProxy<V> fromTable;
  private final TableProxy<V> toTable;
  private final Multiplicity multiplicity;
  private final JoinType joinType;

  public Relationship(Name name, int version, TableProxy<V> fromTable, TableProxy<V> toTable, Multiplicity multiplicity, JoinType joinType) {
    super(name,version);
    this.fromTable = fromTable;
    this.toTable = toTable;
    this.multiplicity = multiplicity;
    this.joinType = joinType;
  }

  @Override
  public String toString() {
    return getId() + ": " + fromTable.getId() + " -> " + toTable.getId()
            + " [" + joinType + "," + multiplicity + "]";
  }

  public enum JoinType {
    PARENT, CHILD, JOIN
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY
  }
}