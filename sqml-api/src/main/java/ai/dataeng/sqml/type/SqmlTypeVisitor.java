package ai.dataeng.sqml.type;

import ai.dataeng.sqml.type.SqmlType.ArraySqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.DateTimeSqmlType;
import ai.dataeng.sqml.type.SqmlType.NullSqmlType;
import ai.dataeng.sqml.type.SqmlType.NumberSqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;
import ai.dataeng.sqml.type.SqmlType.UnknownSqmlType;

public class SqmlTypeVisitor<R, C> {
  public R visitSqmlType(SqmlType type, C context) {
    return null;
  }
  public R visitArray(ArraySqmlType type, C context) {
    return visitSqmlType(type, context);
  }
  public R visitScalarType(SqmlType type, C context) {
    return visitSqmlType(type, context);
  }
  public R visitRelation(RelationSqmlType type, C context) {
    return visitSqmlType(type, context);
  }
  public R visitString(StringSqmlType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitBoolean(BooleanSqmlType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitNumber(NumberSqmlType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitUnknown(UnknownSqmlType type, C context) {
    return visitSqmlType(type, context);
  }
  public R visitDateTime(DateTimeSqmlType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitNull(NullSqmlType type, C context) {
    return visitSqmlType(type, context);
  }
}
