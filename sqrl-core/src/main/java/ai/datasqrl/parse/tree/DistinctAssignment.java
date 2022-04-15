package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Optional;

public class DistinctAssignment extends Assignment {

  private final Name table;
  private final List<Name> partitionKeys;
  private final List<SortItem> order;

  public DistinctAssignment(Optional<NodeLocation> location,
      NamePath name, Name table, List<Name> fields, List<SortItem> order) {
    super(location, name);
    this.table = table;
    this.partitionKeys = fields;
    this.order = order;
  }

  public Name getTable() {
    return table;
  }

  public List<Name> getPartitionKeys() {
    return partitionKeys;
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
