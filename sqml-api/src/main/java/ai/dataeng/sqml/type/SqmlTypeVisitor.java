package ai.dataeng.sqml.type;

import ai.dataeng.sqml.type.RelationType.ImportRelationType;
import ai.dataeng.sqml.type.RelationType.NamedRelationType;
import ai.dataeng.sqml.type.RelationType.RootRelationType;

public class SqmlTypeVisitor<R, C> {
  public R visitSqmlType(Type type, C context) {
    return null;
  }
  public R visitArray(ArrayType type, C context) {
    return visitSqmlType(type, context);
  }
  public R visitScalarType(Type type, C context) {
    return visitSqmlType(type, context);
  }
  public R visitRelation(RelationType type, C context) {
    return visitSqmlType(type, context);
  }
  public R visitString(StringType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitBoolean(BooleanType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitNumber(NumberType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitUnknown(UnknownType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitDateTime(DateTimeType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitNull(NullType type, C context) {
    return visitScalarType(type, context);
  }
  public R visitUuid(UuidType type, C context) {
    return visitScalarType(type, context);
  }

  public R visitFloat(FloatType type, C context) {
    return visitNumber(type, context);
  }
  public R visitInteger(IntegerType type, C context) {
    return visitNumber(type, context);
  }

  public R visitNamedRelation(NamedRelationType type, C context) {
    return visitRelation(type, context);
  }
  public R visitRootRelation(RootRelationType type, C context) {
    return visitRelation((RelationType)type, context);
  }  public R visitImportRelation(ImportRelationType type, C context) {
    return visitNamedRelation(type, context);
  }
}
