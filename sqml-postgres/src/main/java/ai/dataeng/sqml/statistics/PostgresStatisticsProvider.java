package ai.dataeng.sqml.statistics;

public class PostgresStatisticsProvider {
  public static Builder newStatisticsProvider() {
    return new Builder();
  }

  public static class Builder {

    public PostgresStatisticsProvider build() {
      return new PostgresStatisticsProvider();
    }
  }
}
