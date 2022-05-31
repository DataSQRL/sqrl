package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class DistinctAssignment extends Assignment {

  private final Name table;
  private final List<Name> partitionKeys;
  private final List<SortItem> order;
  private final List<Hint> hints;

  public DistinctAssignment(Optional<NodeLocation> location,
      NamePath name, Name table, List<Name> fields, List<SortItem> order, List<Hint> hints) {
    super(location, name);
    this.table = table;
    this.partitionKeys = fields;
    this.order = order;
    this.hints = hints;
  }

  public Name getTable() {
    return table;
  }

  public List<Hint> getHints() {
    return hints;
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DistinctAssignment that = (DistinctAssignment) o;
    return Objects.equals(table, that.table) && Objects.equals(partitionKeys,
        that.partitionKeys) && Objects.equals(order, that.order);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, partitionKeys, order);
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDistinctAssignment(this, context);
  }

}
