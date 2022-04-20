package ai.datasqrl.validate.scopes;

import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Table;
import ai.datasqrl.validate.Namespace;
import java.util.List;
import lombok.Value;

@Value
public class DistinctScope implements ValidatorScope {
  Table table;
  List<Field> partitionKeys;
  List<Field> sortFields;
  @Override
  public Namespace getNamespace() {
    return null;
  }
}
