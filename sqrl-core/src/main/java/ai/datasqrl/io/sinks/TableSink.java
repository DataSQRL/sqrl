package ai.datasqrl.io.sinks;

import ai.datasqrl.parse.tree.name.Name;
import lombok.Value;

@Value
public class TableSink {

  private final Name name;
  private final DataSink sink;

}
