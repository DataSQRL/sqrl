package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.VersionedName;
import lombok.Getter;
import lombok.Setter;

@Getter
public class Relationship extends Field {

  private final Table table;
  public final Table toTable;
  public final Type type;
  @Setter
  public Node node;

  public final Multiplicity multiplicity;

  public int version = 0;

  public Relationship(Name name, Table fromTable, Table toTable, Type type,
      Multiplicity multiplicity) {
    super(name);
    this.table = fromTable;
    this.toTable = toTable;
    this.type = type;
    this.multiplicity = multiplicity;
  }

  @Override
  public VersionedName getId() {
    return VersionedName.of(name, version);
  }

  @Override
  public int getVersion() {
    return version;
  }

  public Node getNode() {
    return node;
  }

  public enum Type {
    PARENT, CHILD, JOIN
  }

  public enum Multiplicity {
    ZERO_ONE, ONE, MANY
  }
}