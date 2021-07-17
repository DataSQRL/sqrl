package ai.dataeng.sqml.statistics;

import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlType;
import java.util.HashMap;
import java.util.Map;

public class StaticStatisticsProvider extends StatisticsProvider {
  private final Map<QualifiedName, SqmlType> columns;

  public StaticStatisticsProvider(Map<QualifiedName, SqmlType> columns) {
    this.columns = columns;
  }

  public Map<QualifiedName, SqmlType> getColumns() {
    return columns;
  }

  public static Builder newStatisticsProvider() {
    return new Builder();
  }

  public static class Builder extends StatisticsProvider.Builder {
    Map<QualifiedName, SqmlType> columns = new HashMap<>();
    public StatisticsProvider build() {
      return new StaticStatisticsProvider(columns);
    }

    public Builder addColumn(QualifiedName name, SqmlType type) {
      columns.put(name, type);
      return this;
    }
  }
}