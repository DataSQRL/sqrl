package ai.dataeng.sqml.type;

import ai.dataeng.sqml.type.SqmlType.ArraySqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.FloatSqmlType;
import ai.dataeng.sqml.type.SqmlType.IntegerSqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;

public class SqmlTypeVisitor<R, C> {

  public R visit(ArraySqmlType type, C context) {
    return null;
  }
  public R visit(FloatSqmlType type, C context) {
    return null;
  }
  public R visit(IntegerSqmlType type, C context) {
    return null;
  }
  public R visit(StringSqmlType type, C context) {
    return null;
  }
  public R visit(BooleanSqmlType type, C context) {
    return null;
  }
}
