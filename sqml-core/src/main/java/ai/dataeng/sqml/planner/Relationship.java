package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.tree.InlineJoin;
import ai.dataeng.sqml.tree.name.Name;
import java.util.Optional;
import lombok.Getter;

@Getter
public class Relationship extends Field {

  public final Table toTable;
  public final Type type;
  public final Multiplicity multiplicity;
  private final Optional<InlineJoin> joinNode;

  public Relationship(
      Name name, Table fromTable, Table toTable, Type type, Multiplicity multiplicity) {
    this(name, fromTable, toTable, type, multiplicity, Optional.empty());
  }
  public Relationship(
      Name name, Table fromTable, Table toTable, Type type, Multiplicity multiplicity,
      Optional<InlineJoin> joinNode) {
    super(name, fromTable);
    this.toTable = toTable;
    this.type = type;
    this.multiplicity = multiplicity;
    this.joinNode = joinNode;
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
