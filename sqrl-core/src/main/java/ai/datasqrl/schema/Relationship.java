package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.name.Name;
import java.util.Optional;
import lombok.Getter;

@Getter
public class Relationship extends Field {

  private final Table fromTable;
  private final Table toTable; //todo rename to TargetTable
  private final JoinType joinType;
  private final Multiplicity multiplicity;

  private final Optional<OrderBy> orders;
  //Requires a transformation to limit cardinality to one
  private final Optional<Limit> limit;

  public Relationship(Name name, Table fromTable, Table toTable, JoinType joinType,
                      Multiplicity multiplicity, Optional<OrderBy> orders,
                      Optional<Limit> limit) {
    super(name);
    this.fromTable = fromTable;
    this.toTable = toTable;
    this.joinType = joinType;
    this.multiplicity = multiplicity;
    this.orders = orders;
    this.limit = limit;
  }

  public Relationship(Name name, Table fromTable, Table toTable, JoinType joinType,
                      Multiplicity multiplicity) {
    this(name, fromTable,toTable,joinType,multiplicity, Optional.empty(), Optional.empty());
  }

  @Override
  public Name getId() {
    return name;
  }

  @Override
  public int getVersion() {
    return 0;
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