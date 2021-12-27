package org.apache.calcite;

import ai.dataeng.sqml.type.basic.BasicType;
import ai.dataeng.sqml.type.basic.BigIntegerType;
import ai.dataeng.sqml.type.basic.DateTimeType;
import ai.dataeng.sqml.type.basic.IntegerType;
import ai.dataeng.sqml.type.basic.StringType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

public class ToBasicType implements LogicalTypeVisitor<BasicType> {
  @Override
  public BasicType visit(CharType charType) {
    return StringType.INSTANCE;
  }

  @Override
  public BasicType visit(VarCharType varCharType) {
    return StringType.INSTANCE;
  }

  @Override
  public BasicType visit(BooleanType booleanType) {
    return ai.dataeng.sqml.type.basic.BooleanType.INSTANCE;
  }

  @Override
  public BasicType visit(BinaryType binaryType) {
    return null;
  }

  @Override
  public BasicType visit(VarBinaryType varBinaryType) {
    return null;
  }

  @Override
  public BasicType visit(DecimalType decimalType) {
    return ai.dataeng.sqml.type.basic.FloatType.INSTANCE;
  }

  @Override
  public BasicType visit(TinyIntType tinyIntType) {
    return IntegerType.INSTANCE;
  }

  @Override
  public BasicType visit(SmallIntType smallIntType) {
    return IntegerType.INSTANCE;
  }

  @Override
  public BasicType visit(IntType intType) {
    return IntegerType.INSTANCE;
  }

  @Override
  public BasicType visit(BigIntType bigIntType) {
    return BigIntegerType.INSTANCE;
  }

  @Override
  public BasicType visit(FloatType floatType) {
    return ai.dataeng.sqml.type.basic.FloatType.INSTANCE;
  }

  @Override
  public BasicType visit(DoubleType doubleType) {
    return ai.dataeng.sqml.type.basic.FloatType.INSTANCE;
  }

  @Override
  public BasicType visit(DateType dateType) {
    return DateTimeType.INSTANCE;
  }

  @Override
  public BasicType visit(TimeType timeType) {
    return DateTimeType.INSTANCE;
  }

  @Override
  public BasicType visit(TimestampType timestampType) {
    return DateTimeType.INSTANCE;
  }

  @Override
  public BasicType visit(ZonedTimestampType zonedTimestampType) {
    return DateTimeType.INSTANCE;
  }

  @Override
  public BasicType visit(LocalZonedTimestampType localZonedTimestampType) {
    return DateTimeType.INSTANCE;
  }

  @Override
  public BasicType visit(YearMonthIntervalType yearMonthIntervalType) {
    return null;
  }

  @Override
  public BasicType visit(DayTimeIntervalType dayTimeIntervalType) {
    return null;
  }

  @Override
  public BasicType visit(ArrayType arrayType) {
    return null;
  }

  @Override
  public BasicType visit(MultisetType multisetType) {
    return null;
  }

  @Override
  public BasicType visit(MapType mapType) {
    return null;
  }

  @Override
  public BasicType visit(RowType rowType) {
    return null;
  }

  @Override
  public BasicType visit(DistinctType distinctType) {
    return null;
  }

  @Override
  public BasicType visit(StructuredType structuredType) {
    return null;
  }

  @Override
  public BasicType visit(NullType nullType) {
    return null;
  }

  @Override
  public BasicType visit(RawType<?> rawType) {
    return null;
  }

  @Override
  public BasicType visit(SymbolType<?> symbolType) {
    return null;
  }

  @Override
  public BasicType visit(LogicalType logicalType) {
    return null;
  }
}