package ai.datasqrl.api;

import ai.datasqrl.io.sources.DataSourceImplementation;
import lombok.Value;

public class InputModel {

  @Value
  public static class DataSource {
    String name;
    DataSourceImplementation source;
  }
}
