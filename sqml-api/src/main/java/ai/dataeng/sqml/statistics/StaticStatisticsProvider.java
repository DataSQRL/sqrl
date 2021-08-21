package ai.dataeng.sqml.statistics;

import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.Type;
import java.util.HashMap;
import java.util.Map;

public class StaticStatisticsProvider extends StatisticsProvider {
  private final Map<QualifiedName, Type> columns;

  public StaticStatisticsProvider(Map<QualifiedName, Type> columns) {
    this.columns = columns;
  }

  public Map<QualifiedName, Type> getColumns() {
    return columns;
  }

  public static Builder newStatisticsProvider() {
    return new Builder();
  }

  public static class Builder extends StatisticsProvider.Builder {
    Map<QualifiedName, Type> columns = new HashMap<>();
    public StatisticsProvider build() {
      return new StaticStatisticsProvider(columns);
    }

    public Builder addColumn(QualifiedName name, Type type) {
      columns.put(name, type);
      return this;
    }
  }
}