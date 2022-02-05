package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.tree.InlineJoin;
import ai.dataeng.sqml.tree.name.Name;
import java.util.List;
import java.util.Optional;
import lombok.Getter;

@Getter
public class Relationship extends Field {

  public final Table toTable;
  public final Type type;
  public final Multiplicity multiplicity;
  private final Optional<List<Field>> lhs;
  private final Optional<List<Field>> rhs;

  public Relationship(
      Name name, Table fromTable, Table toTable, Type type, Multiplicity multiplicity) {
    this(name, fromTable, toTable, type, multiplicity, Optional.empty(), Optional.empty());
  }
  public Relationship(
      Name name, Table fromTable, Table toTable, Type type, Multiplicity multiplicity, Optional<List<Field>> lhs, Optional<List<Field>> rhs) {
    super(name, fromTable);
    this.toTable = toTable;
    this.type = type;
    this.multiplicity = multiplicity;
    this.lhs = lhs;
    this.rhs = rhs;
  }

  @Override
  public Field copy() {
    return null;
  }

  //captures the logical representation of the join that defines this relationship

  public List<Field> getFrom() {
    return lhs.orElse(List.of(table.getPrimaryKeys().get(0)));
  }

  public List<Field> getTo() {
    return rhs.orElse(List.of(toTable.getPrimaryKeys().get(0)));
  }

  public enum Type {
    PARENT, CHILD, JOIN;
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY;
  }

}
