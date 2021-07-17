package ai.dataeng.sqml.statistics;

import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlType;
import java.util.Map;

public abstract class StatisticsProvider {
  public abstract Map<QualifiedName, SqmlType> getColumns();

  public abstract static class Builder {
    public abstract StatisticsProvider build();
  }
}
