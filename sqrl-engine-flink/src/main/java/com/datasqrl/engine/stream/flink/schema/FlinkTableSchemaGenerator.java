package com.datasqrl.engine.stream.flink.schema;

import com.datasqrl.schema.UniversalTableBuilder;
import com.datasqrl.schema.input.SqrlTypeConverter;
import com.datasqrl.schema.type.Type;
import com.datasqrl.schema.type.basic.*;
import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

import java.util.List;


@Value
public class FlinkTableSchemaGenerator implements UniversalTableBuilder.TypeConverter<DataType>,
    UniversalTableBuilder.SchemaConverter<Schema> {

  public static final FlinkTableSchemaGenerator INSTANCE = new FlinkTableSchemaGenerator();

  @Override
  public DataType convertBasic(RelDataType datatype) {
    if (datatype instanceof BasicSqlType || datatype instanceof IntervalSqlType) {
      switch (datatype.getSqlTypeName()) {
        case VARCHAR:
          return DataTypes.STRING();
        case CHAR:
          return DataTypes.CHAR(datatype.getPrecision());
        case TIMESTAMP:
          return DataTypes.TIMESTAMP(datatype.getPrecision());
        case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
          return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(datatype.getPrecision());
        case BIGINT:
          return DataTypes.BIGINT();
        case INTEGER:
          return DataTypes.INT();
        case SMALLINT:
          return DataTypes.SMALLINT();
        case TINYINT:
          return DataTypes.TINYINT();
        case BOOLEAN:
          return DataTypes.BOOLEAN();
        case DOUBLE:
          return DataTypes.DOUBLE();
        case DECIMAL:
          return DataTypes.DECIMAL(datatype.getPrecision(), datatype.getScale());
        case FLOAT:
          return DataTypes.FLOAT();
        case REAL:
          return DataTypes.DOUBLE();
        case DATE:
          return DataTypes.DATE();
        case TIME:
          return DataTypes.TIME(datatype.getPrecision());

        case TIME_WITH_LOCAL_TIME_ZONE:
        case BINARY:
        case VARBINARY:
        case GEOMETRY:
        case SYMBOL:
        case ANY:
        case NULL:
        default:
      }
    } else if (datatype instanceof IntervalSqlType) {
      IntervalSqlType interval = (IntervalSqlType) datatype;
      switch (interval.getSqlTypeName()) {
        case INTERVAL_DAY_HOUR:
        case INTERVAL_DAY_MINUTE:
        case INTERVAL_DAY_SECOND:
        case INTERVAL_HOUR_MINUTE:
        case INTERVAL_HOUR_SECOND:
        case INTERVAL_MINUTE_SECOND:
        case INTERVAL_YEAR_MONTH:
          throw new UnsupportedOperationException();
        case INTERVAL_SECOND:
          return DataTypes.INTERVAL(DataTypes.SECOND(datatype.getPrecision()));
        case INTERVAL_YEAR:
          return DataTypes.INTERVAL(DataTypes.YEAR(datatype.getPrecision()));
        case INTERVAL_MINUTE:
          return DataTypes.INTERVAL(DataTypes.MINUTE());
        case INTERVAL_HOUR:
          return DataTypes.INTERVAL(DataTypes.HOUR());
        case INTERVAL_DAY:
          return DataTypes.INTERVAL(DataTypes.SECOND(datatype.getPrecision()));
        case INTERVAL_MONTH:
          return DataTypes.INTERVAL(DataTypes.MONTH());

      }
    }
    throw new UnsupportedOperationException("Unsupported type: " + datatype);
  }

  @Override
  public DataType nullable(DataType type, boolean nullable) {
    if (nullable) {
      return type.nullable();
    } else {
      return type.notNull();
    }
  }

  @Override
  public DataType wrapArray(DataType type) {
    return DataTypes.ARRAY(type);
  }

  @Override
  public DataType nestedTable(List<Pair<String, DataType>> columns) {
    DataTypes.Field[] fields = new DataTypes.Field[columns.size()];
    int i = 0;
    for (Pair<String, DataType> column : columns) {
      fields[i++] = DataTypes.FIELD(column.getKey(), column.getValue());
    }
    return DataTypes.ROW(fields);
  }

  @Override
  public Schema convertSchema(UniversalTableBuilder tblBuilder) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    for (Pair<String, DataType> column : tblBuilder.convert(this)) {
      schemaBuilder.column(column.getKey(), column.getValue());
    }
    return schemaBuilder.build();
  }

  public static class SqrlType2TableConverter implements SqrlTypeConverter<DataType> {

    public static SqrlType2TableConverter INSTANCE = new SqrlType2TableConverter();

    @Override
    public DataType visitType(Type type, Void context) {
      throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public <J> DataType visitBasicType(AbstractBasicType<J> type, Void context) {
      throw new UnsupportedOperationException("Basic type is not supported in Table API: " + type);
    }

    @Override
    public DataType visitBooleanType(BooleanType type, Void context) {
      return DataTypes.BOOLEAN();
    }

    @Override
    public DataType visitDateTimeType(DateTimeType type, Void context) {
      return DataTypes.TIMESTAMP_LTZ(3);
    }

    @Override
    public DataType visitFloatType(FloatType type, Void context) {
      return DataTypes.DOUBLE();
    }

    @Override
    public DataType visitIntegerType(IntegerType type, Void context) {
      return DataTypes.BIGINT();
    }

    @Override
    public DataType visitStringType(StringType type, Void context) {
      return DataTypes.STRING();
    }

    @Override
    public DataType visitUuidType(UuidType type, Void context) {
      return DataTypes.CHAR(36);
    }

    @Override
    public DataType visitIntervalType(IntervalType type, Void context) {
      return DataTypes.INTERVAL(DataTypes.SECOND(3));
    }
  }

}
