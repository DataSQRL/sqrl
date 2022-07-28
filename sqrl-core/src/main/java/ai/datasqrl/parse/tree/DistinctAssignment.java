package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.logging.log4j.util.Strings;

public class DistinctAssignment extends Assignment {

  private final TableNode table;
  private final Optional<Identifier> alias;
  private final List<Identifier> partitionKeys;
  private final List<SortItem> order;
  private final List<Hint> hints;

  public DistinctAssignment(Optional<NodeLocation> location, NamePath name, TableNode table,
      Optional<Identifier> alias,
      List<Identifier> partitionKeys, List<SortItem> order, List<Hint> hints) {
    super(location, name);
    this.table = table;
    this.alias = alias;
    this.partitionKeys = partitionKeys;
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

  public List<Identifier> getPartitionKeys() {
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
    return Objects.equals(table, that.table) && Objects.equals(partitionKeys, that.partitionKeys)
        && Objects.equals(order, that.order);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, partitionKeys, order);
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDistinctAssignment(this, context);
  }

  /**
   * SELECT * /#hint: top 1#/ FROM node.tableName GROUP BY node.pks ORDER BY node.pks
   */
  public String getSqlQuery() {

    String orders = order.stream().map(NodeFormatter::accept).collect(Collectors.joining(", "));

    return String.format(
        "SELECT /*+ DISTINCT_ON(%s) */ * FROM %s %s %s LIMIT 1",
        getPartitionKeys().stream().map(NodeFormatter::accept)
            .map(p->"\""+p+"\"")
            .collect(Collectors.joining(", ")), getTable(),
        alias.map(NodeFormatter::accept).orElse(""),
        Strings.isEmpty(orders) ? "" : "ORDER BY " + orders
       );
  }
}
