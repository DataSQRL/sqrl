package ai.dataeng.sqml.tree;

import java.util.List;
import java.util.Optional;

public class DistinctAssignment extends Assignment {
  private final Identifier table;
  private final List<Identifier> fields;
  private final List<SortItem> order;

  public DistinctAssignment(Optional<NodeLocation> location,
      Identifier table, List<Identifier> fields, List<SortItem> order) {
    super(location);

    this.table = table;
    this.fields = fields;
    this.order = order;
  }

  public Identifier getTable() {
    return table;
  }

  public List<Identifier> getFields() {
    return fields;
  }

  public List<SortItem> getOrder() {
    return order;
  }

  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDistinctAssignment(this, context);
  }
}
