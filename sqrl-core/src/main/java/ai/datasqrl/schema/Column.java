package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import lombok.Getter;
import lombok.Value;

@Getter
public class Column extends Field {

  final boolean isVisible;

  Column(Name name, int version, boolean isVisible) {
    super(name, version);
    this.isVisible = isVisible;
  }

  @Override
  public String toString() {
    return super.toString();
  }

}
