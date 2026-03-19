/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.calcite.type;

import jakarta.inject.Singleton;
import java.lang.reflect.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.springframework.stereotype.Component;

@Component
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

  public static RelDataType withNullable(
      RelDataTypeFactory typeFactory, RelDataType type, boolean nullable) {
    return typeFactory.createTypeWithNullability(type, nullable);
  }

  public static RelDataType wrapInArray(RelDataTypeFactory typeFactory, RelDataType type) {
    return typeFactory.createArrayType(type, -1L);
  }

  public RelDataType wrapInArray(RelDataType type) {
    return wrapInArray(this, type);
  }

  /** Provides mapping to java types for execution */
  @Override
  public Type getJavaClass(RelDataType type) {
    return super.getJavaClass(type);
  }
}
