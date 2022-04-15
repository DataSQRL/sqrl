package ai.datasqrl.validate;

import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.Table;
import ai.datasqrl.validate.Scope.ResolveResult;
import ai.datasqrl.parse.tree.name.Name;

public class PathUtil {
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
