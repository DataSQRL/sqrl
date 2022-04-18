package ai.datasqrl.validate.paths;

import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Table;

/**
 * From schema object
 */
public class BaseTablePath implements TablePath {

  public static BaseTablePath resolve(Table table, NamePath popFirst) {
    return null;
  }
}
