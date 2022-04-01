package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.tree.name.Name;
import lombok.Getter;

@Getter
public class TypelessField extends Field {

  private final int version;
  private boolean isPrimaryKey;

  public TypelessField(Name name, int version) {
    super(name);
    this.version = version;
  }

  public void setPrimaryKey(boolean isPrimaryKey) {

    this.isPrimaryKey = isPrimaryKey;
  }
}
