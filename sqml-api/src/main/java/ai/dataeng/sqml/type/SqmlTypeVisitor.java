package ai.dataeng.sqml.type;

import ai.dataeng.sqml.type.SqmlType.ArraySqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.FloatSqmlType;
import ai.dataeng.sqml.type.SqmlType.IntegerSqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;

public class SqmlTypeVisitor<R, C> {
  public R visitSqmlType(SqmlType type, C context) {
    return null;
  }
  public R visitScalarType(SqmlType type, C context) {
    return visitSqmlType(type, context);
  }
  public R visitRelation(RelationSqmlType type, C context) {
    return visitSqmlType(type, context);
  }
  public R visitArray(ArraySqmlType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitFloat(FloatSqmlType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitInteger(IntegerSqmlType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitString(StringSqmlType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitBoolean(BooleanSqmlType type, C context) {
    return visitScalarType(type, context);
  }

}
