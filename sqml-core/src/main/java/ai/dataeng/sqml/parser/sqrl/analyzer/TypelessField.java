package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.tree.name.Name;

public class TypelessField extends Field {

  private final int version;

  public TypelessField(Name name, int version) {
    super(name);
    this.version = version;
  }

  @Override
  public int getVersion() {
    return 0;
  }
}
