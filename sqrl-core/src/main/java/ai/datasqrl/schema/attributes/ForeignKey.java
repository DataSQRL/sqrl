package ai.datasqrl.schema.attributes;

import ai.datasqrl.schema.Attribute;
import ai.datasqrl.schema.Column;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

@AllArgsConstructor
@Getter
public class ForeignKey extends Attribute {
  public Column references;
}
