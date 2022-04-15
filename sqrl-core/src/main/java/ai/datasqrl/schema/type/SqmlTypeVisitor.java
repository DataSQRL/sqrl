package ai.datasqrl.schema.type;


import ai.datasqrl.schema.type.basic.AbstractBasicType;
import ai.datasqrl.schema.type.basic.BigIntegerType;
import ai.datasqrl.schema.type.basic.BooleanType;
import ai.datasqrl.schema.type.basic.DateTimeType;
import ai.datasqrl.schema.type.basic.DoubleType;
import ai.datasqrl.schema.type.basic.FloatType;
import ai.datasqrl.schema.type.basic.IntegerType;
import ai.datasqrl.schema.type.basic.IntervalType;
import ai.datasqrl.schema.type.basic.NullType;
import ai.datasqrl.schema.type.basic.NumberType;
import ai.datasqrl.schema.type.basic.StringType;
import ai.datasqrl.schema.type.basic.UuidType;

public class SqmlTypeVisitor<R, C> {
  public R visitType(Type type, C context) {
    return null;
  }
  public <J> R visitBasicType(AbstractBasicType<J> type, C context) {
    return visitType(type, context);
  }
  public R visitBooleanType(BooleanType type, C context) {
    return visitBasicType(type, context);
  }
  public R visitDateTimeType(DateTimeType type, C context) {
    return visitBasicType(type, context);
  }
  public R visitFloatType(FloatType type, C context) {
    return visitBasicType(type, context);
  }
  public R visitIntegerType(IntegerType type, C context) {
    return visitBasicType(type, context);
  }
  public R visitNullType(NullType type, C context) {
    return visitBasicType(type, context);
  }
  public R visitNumberType(NumberType type, C context) {
    return visitBasicType(type, context);
  }
  public R visitStringType(StringType type, C context) {
    return visitBasicType(type, context);
  }
  public R visitUuidType(UuidType type, C context) {
    return visitBasicType(type, context);
  }
  public R visitArrayType(ArrayType type, C context) {
    return visitType(type, context);
  }
  public R visitBigIntegerType(BigIntegerType type, C context) {
    return visitBasicType(type, context);
  }
  public R visitIntervalType(IntervalType type, C context) {
    return visitBasicType(type, context);
  }
  public R visitDoubleType(DoubleType type, C context) {
    return visitBasicType(type, context);
  }
}
