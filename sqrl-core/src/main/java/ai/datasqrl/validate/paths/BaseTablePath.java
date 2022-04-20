package ai.datasqrl.validate.paths;

import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Table;
import lombok.Value;

@Value
public class BaseTablePath implements TablePath {
  Table table;
  NamePath namePath;

  public static BaseTablePath resolve(Table table, NamePath namePath) {
    return new BaseTablePath(table, namePath);
  }
}
