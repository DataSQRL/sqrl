package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class DistinctAssignment extends Assignment {

  private final TableNode table;
  private final List<Name> partitionKeys;
  private final List<SortItem> order;
  private final List<Hint> hints;
  private final List<Identifier> partitionKeyNodes;

  public DistinctAssignment(Optional<NodeLocation> location,
      NamePath name, TableNode table, List<Identifier> fields, List<SortItem> order, List<Hint> hints) {
    super(location, name);
    this.table = table;
    this.partitionKeys = fields.stream().map(e->e.getNamePath().getLast()).collect(Collectors.toList());
    this.partitionKeyNodes = fields;
    this.order = order;
    this.hints = hints;
  }

  @Deprecated
  public Name getTable() {
    return table.getNamePath().getFirst();
  }

  public TableNode getTableNode() {
    return table;
  }

  public List<Hint> getHints() {
    return hints;
  }

  @Deprecated
  public List<Name> getPartitionKeys() {
    return partitionKeys;
  }

  public List<Identifier> getPartitionKeyNodes() {
    return partitionKeyNodes;
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
