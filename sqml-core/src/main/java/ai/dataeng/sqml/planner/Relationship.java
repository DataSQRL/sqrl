package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.tree.name.Name;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

@Getter
public class Relationship extends Field {

  public final Table toTable;
  public final Type type;
  public final Multiplicity multiplicity;
  private final RelNode node;

  public Relationship(
      Name name, Table fromTable, Table toTable, Type type, Multiplicity multiplicity, RelNode node) {
    super(name, fromTable);
    this.toTable = toTable;
    this.type = type;
    this.multiplicity = multiplicity;
    this.node = node;
  }

  @Override
  public Field copy() {
    return null;
  }

  //captures the logical representation of the join that defines this relationship

  public List<Field> getFrom() {
    return null;
  }

  public List<Field> getTo() {
    return null;
  }

  public enum Type {
    PARENT, CHILD, JOIN
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY
  }

}
