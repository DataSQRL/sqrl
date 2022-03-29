package ai.dataeng.sqml.parser.sqrl;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.sqrl.analyzer.TypelessField;
import ai.dataeng.sqml.tree.name.Name;

public class FieldFactory {

  public static Field createTypeless(Table table, Name name) {
    if (table.getField(name) != null) {
      return new TypelessField(name, table.getField(name).getVersion() + 1);
    } else {
      return new TypelessField(name, 0);
    }
  }
}
