package ai.dataeng.sqml.type;


import ai.dataeng.sqml.type.basic.AbstractBasicType;
import ai.dataeng.sqml.type.basic.BigIntegerType;
import ai.dataeng.sqml.type.basic.BooleanType;
import ai.dataeng.sqml.type.basic.DateTimeType;
import ai.dataeng.sqml.type.basic.DoubleType;
import ai.dataeng.sqml.type.basic.FloatType;
import ai.dataeng.sqml.type.basic.IntegerType;
import ai.dataeng.sqml.type.basic.IntervalType;
import ai.dataeng.sqml.type.basic.NullType;
import ai.dataeng.sqml.type.basic.NumberType;
import ai.dataeng.sqml.type.basic.StringType;
import ai.dataeng.sqml.type.basic.UuidType;

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
