package ai.dataeng.sqml.execution.sql;

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

public class SqlTypeConverter implements LogicalTypeVisitor<String> {

    @Override
    public String visit(CharType charType) {
      return "VARCHAR";
    }

    @Override
    public String visit(VarCharType varCharType) {
      return "VARCHAR";
    }

    @Override
    public String visit(BooleanType booleanType) {
      return "BOOLEAN";
    }

    @Override
    public String visit(BinaryType binaryType) {
      return "BYTES";
    }

    @Override
    public String visit(VarBinaryType varBinaryType) {
      return "BYTES";
    }

    @Override
    public String visit(DecimalType decimalType) {
      return "DECIMAL";
    }

    @Override
    public String visit(TinyIntType tinyIntType) {
      return "INT";
    }

    @Override
    public String visit(SmallIntType smallIntType) {
      return "INT";
    }

    @Override
    public String visit(IntType intType) {
      return "INT";
    }

    @Override
    public String visit(BigIntType bigIntType) {
      return "BIGINT";
    }

    @Override
    public String visit(FloatType floatType) {
      return "FLOAT";
    }

    @Override
    public String visit(DoubleType doubleType) {
      return "DECIMAL";
    }

    @Override
    public String visit(DateType dateType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(TimeType timeType) {
      return "TIME";
    }

    @Override
    public String visit(TimestampType timestampType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(ZonedTimestampType zonedTimestampType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(LocalZonedTimestampType localZonedTimestampType) {
      return "TIMESTAMP";
    }

    @Override
    public String visit(YearMonthIntervalType yearMonthIntervalType) {
      return "VARCHAR";
    }

    @Override
    public String visit(DayTimeIntervalType dayTimeIntervalType) {
      return "VARCHAR";
    }

    @Override
    public String visit(ArrayType arrayType) {
      return null;
    }

    @Override
    public String visit(MultisetType multisetType) {
      return null;
    }

    @Override
    public String visit(MapType mapType) {
      return null;
    }

    @Override
    public String visit(RowType rowType) {
      return null;
    }

    @Override
    public String visit(DistinctType distinctType) {
      return null;
    }

    @Override
    public String visit(StructuredType structuredType) {
      return null;
    }

    @Override
    public String visit(NullType nullType) {
      return null;
    }

    @Override
    public String visit(RawType<?> rawType) {
      return null;
    }

    @Override
    public String visit(SymbolType<?> symbolType) {
      return null;
    }

    @Override
    public String visit(LogicalType logicalType) {
      return null;
    }
  }