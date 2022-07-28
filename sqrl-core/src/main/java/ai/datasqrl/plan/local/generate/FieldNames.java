package ai.datasqrl.plan.local.generate;

import ai.datasqrl.schema.Field;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;

public class FieldNames {

  Map<Field, String> fieldNames = new HashMap<>();

  public String get(Field field) {
    //for debug
    if (!fieldNames.containsKey(field)) {
      System.out.println("Could not map field during transpilation: " + field);
      return field.getName().getDisplay();
    }
    Preconditions.checkState(fieldNames.containsKey(field), "Could not find field ", field);
    return fieldNames.get(field);
  }

  public void put(Field field, String name) {
    fieldNames.put(field, name);
  }

  public void putAll(Map<Field, String> fieldNameMap) {
    fieldNames.putAll(fieldNameMap);
  }
}
