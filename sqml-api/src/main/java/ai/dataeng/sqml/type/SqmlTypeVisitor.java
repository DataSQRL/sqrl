package ai.dataeng.sqml.type;


import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.AbstractBasicType;
import ai.dataeng.sqml.schema2.basic.BooleanType;
import ai.dataeng.sqml.schema2.basic.DateTimeType;
import ai.dataeng.sqml.schema2.basic.FloatType;
import ai.dataeng.sqml.schema2.basic.IntegerType;
import ai.dataeng.sqml.schema2.basic.NullType;
import ai.dataeng.sqml.schema2.basic.NumberType;
import ai.dataeng.sqml.schema2.basic.StringType;
import ai.dataeng.sqml.schema2.basic.UuidType;

public class SqmlTypeVisitor<R, C> {
  public <F extends Field> R visitRelation(RelationType<F> relationType, C context) {
    return null;
  }
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

  //Todo: other field types
}
