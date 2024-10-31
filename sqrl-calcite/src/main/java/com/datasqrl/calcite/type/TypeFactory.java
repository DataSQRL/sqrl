
package com.datasqrl.calcite.type;

import com.google.inject.Singleton;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@Singleton
public class TypeFactory extends FlinkTypeFactory {

  public TypeFactory() {
    super(TypeFactory.class.getClassLoader(), FlinkTypeSystem.INSTANCE);
  }

  public static TypeFactory getTypeFactory() {
    return new TypeFactory();
  }

  public static RelDataType makeTimestampType(RelDataTypeFactory typeFactory) {
    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);
  }

  public static RelDataType makeTimestampType(RelDataTypeFactory typeFactory, boolean nullable) {
    return withNullable(typeFactory, makeTimestampType(typeFactory), nullable);
  }

  public static RelDataType makeUuidType(RelDataTypeFactory typeFactory) {
    return typeFactory.createSqlType(SqlTypeName.CHAR, 36);
  }

  public static RelDataType makeUuidType(RelDataTypeFactory typeFactory, boolean nullable) {
    return withNullable(typeFactory, makeUuidType(typeFactory), nullable);
  }

  public static RelDataType makeIntegerType(RelDataTypeFactory typeFactory, boolean nullable) {
    return withNullable(typeFactory, typeFactory.createSqlType(SqlTypeName.INTEGER), nullable);
  }

  public static RelDataType withNullable(RelDataTypeFactory typeFactory, RelDataType type, boolean nullable) {
    return typeFactory.createTypeWithNullability(type, nullable);
  }

  public static RelDataType wrapInArray(RelDataTypeFactory typeFactory, RelDataType type) {
    return typeFactory.createArrayType(type, -1L);
  }

  public RelDataType wrapInArray(RelDataType type) {
    return wrapInArray(this,type);
  }

  /**
   * Provides mapping to java types for execution
   */
  @Override
  public Type getJavaClass(RelDataType type) {
    return super.getJavaClass(type);
  }
}
