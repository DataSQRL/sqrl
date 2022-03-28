package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.tree.name.Name;
import org.apache.flink.table.types.AbstractDataType;

public class FlinkDelegatingField extends Field {

  private final AbstractDataType<?> dataType;

  public FlinkDelegatingField(Name name, AbstractDataType<?> dataType) {
    super(name, null);
    this.dataType = dataType;
  }

  @Override
  public int getVersion() {
    return 0;
  }
}
