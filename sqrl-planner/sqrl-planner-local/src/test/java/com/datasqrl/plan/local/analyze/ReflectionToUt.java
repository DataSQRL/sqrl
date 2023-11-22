package com.datasqrl.plan.local.analyze;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.UniversalTable.Configuration;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.util.RelDataTypeBuilder;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

public class ReflectionToUt {

  public static UniversalTable reflection(Class<?> obj) {
    return makeTable(obj.getSimpleName(), reflectionToRelType(obj));
  }

  private static RelDataType reflectionToRelType(Class<?> obj) {
    TypeFactory typeFactory = TypeFactory.getTypeFactory();
    RelDataTypeBuilder typeBuilder = CalciteUtil.getRelTypeBuilder(typeFactory);
    java.lang.reflect.Field[] fields = obj.getDeclaredFields();
    for (java.lang.reflect.Field field : fields) {
      if (field.getName().equalsIgnoreCase("this$0")) {
        continue;
      }
      if (field.getType().equals(Optional.class)) {
        if (field.getGenericType() instanceof ParameterizedType) {
          ParameterizedType optionalType = (ParameterizedType) field.getGenericType();
          typeBuilder.add(field.getName(),
              typeFactory.createTypeWithNullability(
                      getSqlTypeName(typeFactory, (Class<?>) optionalType.getActualTypeArguments()[0]),
                  true));
        } else {
          throw new RuntimeException("unknown generic type");
        }
      } else if (field.getType().equals(List.class)) {
        if (field.getGenericType() instanceof ParameterizedType) {
          ParameterizedType listType = (ParameterizedType) field.getGenericType();
          Class<?> type = (Class<?>)listType.getActualTypeArguments()[0];
          RelDataType nestedType = typeFactory.wrapInArray(reflectionToRelType(type));
          typeBuilder.add(field.getName(),nestedType);
        } else {
          throw new RuntimeException("unknown generic type");
        }
      } else {
        typeBuilder.add(field.getName(),
            typeFactory.createTypeWithNullability(getSqlTypeName(typeFactory, field.getType()), false));
      }
    }
    return typeBuilder.build();
  }

  private static UniversalTable makeTable(String name, RelDataType type) {
    RelDataTypeFactory typeFactory = TypeFactory.getTypeFactory();
    return UniversalTable.of(type, NamePath.of(name),
        Configuration.forImport(false), 1,
        typeFactory);
  }

  private static RelDataType getSqlTypeName(RelDataTypeFactory typeFactory, Class<?> type) {
    switch (type.getSimpleName().toUpperCase()) {
      case "STRING":
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case "INTEGER":
        return typeFactory.createSqlType(SqlTypeName.INTEGER);
      case "INT":
      case "LONG":
        return typeFactory.createSqlType(SqlTypeName.BIGINT);
      case "DOUBLE":
      case "FLOAT":
        return typeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 5);
      case "LOCALDATETIME":
      case "ZONEDDATETIME":
        return typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 3);
      default:
        throw new RuntimeException("Unknown type: " + type.getSimpleName());
    }
  }
}
