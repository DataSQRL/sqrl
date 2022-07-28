package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.hints.SqrlHintStrategyTable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.logging.log4j.util.Strings;

@Getter
public class DistinctAssignment extends Assignment {

  private final String table;
  private final Optional<String> alias;
  private final List<String> partitionKeys;
  private final String order;
  private final List<Hint> hints;

  public DistinctAssignment(Optional<NodeLocation> location, NamePath name, String table,
      Optional<String> alias, List<String> partitionKeys, String order, List<Hint> hints) {
    super(location, name);
    this.table = table;
    this.alias = alias;
    this.partitionKeys = partitionKeys;
    this.order = order;
    this.hints = hints;
  }

  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDistinctAssignment(this, context);
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
    return Objects.equals(table, that.table) && Objects.equals(alias, that.alias) && Objects.equals(
        partitionKeys, that.partitionKeys) && Objects.equals(order, that.order) && Objects.equals(
        hints, that.hints);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, alias, partitionKeys, order, hints);
  }

  public String getSqlQuery() {
    String pk = getPartitionKeys().stream().map(e -> "\"" + e + "\"")
        .collect(Collectors.joining(", "));
    String order = Strings.isEmpty(getOrder()) ? "" : "ORDER BY " + getOrder();
    String table = getTable();
    return String.format("SELECT /*+ %s(%s) */ * FROM %s %s %s LIMIT 1",
        SqrlHintStrategyTable.TOP_N, pk, table, alias.orElse(""), order);
  }
}
