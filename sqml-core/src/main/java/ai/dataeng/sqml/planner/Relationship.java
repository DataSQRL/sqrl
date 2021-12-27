package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.tree.name.Name;
import lombok.Getter;

@Getter
public class Relationship extends Field {

  public final Table toTable;
  public final Type type;
  public final Multiplicity multiplicity;

  public Relationship(
      Name name, Table fromTable, Table toTable, Type type, Multiplicity multiplicity) {
    super(name, fromTable);
    this.toTable = toTable;
    this.type = type;
    this.multiplicity = multiplicity;
  }

  @Override
  public Field copy() {
    return null;
  }

  //captures the logical representation of the join that defines this relationship


  public enum Type {
    PARENT, CHILD, JOIN;
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY;
  }

}
