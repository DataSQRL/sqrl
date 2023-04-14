package com.datasqrl.plan.local.analyze;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.TypeFactory;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.UniversalTable.ImportFactory;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

public class ReflectionToUt {

  public static UniversalTable reflection(Class<?> obj) {
    Function<String, UniversalTable> utFactory = (name) -> {
      RelDataTypeFactory typeFactory = TypeFactory.getTypeFactory();
      ImportFactory importFactory =
          new UniversalTable.ImportFactory(typeFactory, true, false);
      return importFactory.createTable(Name.system(name),
          NamePath.of(name),
          false);
    };

    return reflection(obj, utFactory);
  }

  public static UniversalTable reflection(Class<?> obj, Function<String, UniversalTable> utFactory) {
    boolean source = false;
    RelDataTypeFactory typeFactory = TypeFactory.getTypeFactory();

    UniversalTable table = utFactory.apply(obj.getSimpleName());
    java.lang.reflect.Field[] fields = obj.getDeclaredFields();
    for (java.lang.reflect.Field field : fields) {
      if (field.getName().equalsIgnoreCase("this$0")) {
        continue;
      }
      if (field.getType().equals(Optional.class)) {
        if (field.getGenericType() instanceof ParameterizedType) {
          ParameterizedType optionalType = (ParameterizedType) field.getGenericType();
          table.addColumn(Name.system(field.getName()),
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

          Function<String, UniversalTable> factory2 = (name) -> {
            ImportFactory importFactory =
                new UniversalTable.ImportFactory(typeFactory, true, false);
            return importFactory.createTable(Name.system(field.getName()),
                NamePath.of(field.getName()), table, false);
          };

          UniversalTable table1 = reflection(type, factory2);
          table.addChild(Name.system(field.getName()), table1, Multiplicity.MANY);
          System.out.println();
//            table.addColumn(Name.system(field.getName()),
//                typeFactory.createTypeWithNullability(typeFactory.createSqlType(
//                        getSqlTypeName(listType.getActualTypeArguments()[0].getClass())),
//                    false));
        } else {
          throw new RuntimeException("unknown generic type");
        }
      } else {
        table.addColumn(Name.system(field.getName()),
            typeFactory.createTypeWithNullability(getSqlTypeName(typeFactory, field.getType()), false));
      }
    }
    return table;
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
