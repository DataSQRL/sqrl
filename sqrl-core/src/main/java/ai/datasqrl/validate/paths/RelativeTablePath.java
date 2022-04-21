package ai.datasqrl.validate.paths;

import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Table;
import lombok.Value;

/**
 * From either a self identifier or a join alias
 */
@Value
public class RelativeTablePath implements TablePath {

  Table table;
  NamePath path;

  public static RelativeTablePath resolve(Table table, NamePath path) {
    return new RelativeTablePath(table, path);
  }
}
