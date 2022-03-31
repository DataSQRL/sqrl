package ai.dataeng.sqml.parser.sqrl;

import ai.dataeng.sqml.parser.Field;
import ai.dataeng.sqml.parser.FieldPath;
import ai.dataeng.sqml.parser.Relationship;
import ai.dataeng.sqml.parser.Relationship.Multiplicity;

public class PathUtil {

  public static boolean isToMany(FieldPath path) {
    for (Field field : path.getFields()) {
      if (field instanceof Relationship) {
        Relationship rel = (Relationship) field;
        if (rel.multiplicity == Multiplicity.MANY) {
          return true;
        }
      }
    }
    return false;
  }

  public static boolean isToOne(FieldPath path) {
    for (Field field : path.getFields()) {
      if (field instanceof Relationship) {
        Relationship rel = (Relationship) field;
        if (rel.multiplicity == Multiplicity.ONE) {
          return true;
        }
      }
    }
    return false;
  }
}
