package ai.dataeng.sqml.execution.sql;

import ai.dataeng.execution.table.column.BooleanColumn;
import ai.dataeng.execution.table.column.FloatColumn;
import ai.dataeng.execution.table.column.H2Column;
import ai.dataeng.execution.table.column.IntegerColumn;
import ai.dataeng.execution.table.column.StringColumn;
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

public class H2TableConverter implements LogicalTypeVisitor<H2Column> {

    private final String name;

    public H2TableConverter(String name) {
      this.name = name;
    }
    @Override
    public H2Column visit(CharType charType) {
      return new StringColumn(name, name);
    }

    @Override
    public H2Column visit(VarCharType varCharType) {
      return new StringColumn(name, name);
    }

    @Override
    public H2Column visit(BooleanType booleanType) {
      return new BooleanColumn(name, name);
    }

    @Override
    public H2Column visit(BinaryType binaryType) {
      return null;
    }

    @Override
    public H2Column visit(VarBinaryType varBinaryType) {
      return null;
    }

    @Override
    public H2Column visit(DecimalType decimalType) {
      return new FloatColumn(name, name);
    }

    @Override
    public H2Column visit(TinyIntType tinyIntType) {
      return new IntegerColumn(name, name);
    }

    @Override
    public H2Column visit(SmallIntType smallIntType) {
      return new IntegerColumn(name, name);
    }

    @Override
    public H2Column visit(IntType intType) {
      return new IntegerColumn(name, name);
    }

    @Override
    public H2Column visit(BigIntType bigIntType) {
      return new IntegerColumn(name, name);
    }

    @Override
    public H2Column visit(FloatType floatType) {
      return new FloatColumn(name, name);
    }

    @Override
    public H2Column visit(DoubleType doubleType) {
      return new FloatColumn(name, name);
    }

    @Override
    public H2Column visit(DateType dateType) {
      return null;
    }

    @Override
    public H2Column visit(TimeType timeType) {
      return null;
    }

    @Override
    public H2Column visit(TimestampType timestampType) {
      return null;
    }

    @Override
    public H2Column visit(ZonedTimestampType zonedTimestampType) {
      return null;
    }

    @Override
    public H2Column visit(LocalZonedTimestampType localZonedTimestampType) {
      return null;
    }

    @Override
    public H2Column visit(YearMonthIntervalType yearMonthIntervalType) {
      return null;
    }

    @Override
    public H2Column visit(DayTimeIntervalType dayTimeIntervalType) {
      return null;
    }

    @Override
    public H2Column visit(ArrayType arrayType) {
      return null;
    }

    @Override
    public H2Column visit(MultisetType multisetType) {
      return null;
    }

    @Override
    public H2Column visit(MapType mapType) {
      return null;
    }

    @Override
    public H2Column visit(RowType rowType) {
      return null;
    }

    @Override
    public H2Column visit(DistinctType distinctType) {
      return null;
    }

    @Override
    public H2Column visit(StructuredType structuredType) {
      return null;
    }

    @Override
    public H2Column visit(NullType nullType) {
      return null;
    }

    @Override
    public H2Column visit(RawType<?> rawType) {
      return null;
    }

    @Override
    public H2Column visit(SymbolType<?> symbolType) {
      return null;
    }

    @Override
    public H2Column visit(LogicalType logicalType) {
      return null;
    }
  }