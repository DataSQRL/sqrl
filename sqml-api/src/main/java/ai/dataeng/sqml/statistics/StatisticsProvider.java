package ai.dataeng.sqml.statistics;

import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.Type;
import java.util.Map;

public abstract class StatisticsProvider {
  public abstract Map<QualifiedName, Type> getColumns();

  public abstract static class Builder {
    public abstract StatisticsProvider build();
  }
}
