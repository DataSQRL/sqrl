package ai.datasqrl.api;

import ai.datasqrl.io.sources.DataSourceConnectorConfig;
import lombok.Value;

public class InputModel {

  @Value
  public static class DataSource {
    String name;
    DataSourceConnectorConfig source;
  }
}
