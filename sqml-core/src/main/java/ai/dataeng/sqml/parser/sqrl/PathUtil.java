package ai.dataeng.sqml.parser.sqrl;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.FieldPath;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Relationship.Multiplicity;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.sqrl.analyzer.Scope;
import ai.dataeng.sqml.parser.sqrl.analyzer.Scope.ResolveResult;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;

public class PathUtil {

  public static boolean needsExpanding(NamePath namePath,
      Scope scope) {
    return false;
  }

  public static boolean isToMany(ResolveResult result) {
    Table current = result.getTable();
    for (Name field : result.getRemaining().get().getNames()) {
      if (current.getField(field) instanceof Relationship) {
        Relationship rel = (Relationship) current.getField(field);
        if (rel.multiplicity == Multiplicity.MANY) {
          return true;
        }
      }
    }
    return false;
  }

  public static boolean isToOne(ResolveResult result) {
    return !isToMany(result) && result.getFirstField() instanceof Relationship;
  }
}
