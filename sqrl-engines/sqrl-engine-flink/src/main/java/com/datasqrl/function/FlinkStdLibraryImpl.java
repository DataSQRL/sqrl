/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

import static com.datasqrl.NamespaceObjectUtil.createFunctionFromFlink;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.NamespaceObject;
import com.google.auto.service.AutoService;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlFunction;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

@AutoService(StdLibrary.class)
public class FlinkStdLibraryImpl extends AbstractFunctionModule implements StdLibrary {
  public static final NamePath LIB_NAME = NamePath.of("flink");

  private static final List<String> toExclude = List.of(
//      BuiltInFunctionDefinitions.NOW.getClass().getName(), //use our NOW
      //Provided by calcite std library
      BuiltInFunctionDefinitions.COALESCE.getName(),
      BuiltInFunctionDefinitions.JSON_VALUE.getName(),
      BuiltInFunctionDefinitions.JSON_ARRAY.getName(),
      BuiltInFunctionDefinitions.JSON_EXISTS.getName(),
      BuiltInFunctionDefinitions.JSON_OBJECT.getName(),
      BuiltInFunctionDefinitions.JSON_QUERY.getName(),
      BuiltInFunctionDefinitions.JSON_ARRAYAGG_ABSENT_ON_NULL.getName(),
      BuiltInFunctionDefinitions.JSON_ARRAYAGG_NULL_ON_NULL.getName(),
      BuiltInFunctionDefinitions.JSON_OBJECTAGG_ABSENT_ON_NULL.getName(),
      BuiltInFunctionDefinitions.JSON_OBJECTAGG_NULL_ON_NULL.getName(),
      BuiltInFunctionDefinitions.JSON_STRING.getName(),
      BuiltInFunctionDefinitions.IS_JSON.getName(),
      //Cast not yet supported, SQRL uses a String type that is not yet compatible
      BuiltInFunctionDefinitions.CAST.getName().toUpperCase(),
      //system functions that cannot be converted to bridging functions
      "FLATTEN",
      "GET",
      "WINDOW_START",
      "WINDOW_END",
      "ORDER_ASC",
      "ORDER_DESC",
      "PROCTIME",
      "ROWTIME",
      "OVER",
      "UNBOUNDED_RANGE",
      "UNBOUNDED_ROW",
      "CURRENT_RANGE",
      "CURRENT_ROW",
      "WITH_COLUMNS",
      "WITHOUT_COLUMNS",
      "AS",
      "STREAM_RECORD_TIMESTAMP",
      "RANGE_TO"
  );

  private static List<NamespaceObject> SQL_FUNCTIONS = getAllFunctionsFromFlink();


  public FlinkStdLibraryImpl() {
    super(SQL_FUNCTIONS);
  }

  public static List<NamespaceObject> getAllFunctionsFromFlink() {
    List<NamespaceObject> functions = new ArrayList<>();

    // Get all fields from FlinkSqlOperatorTable
    Field[] fields = BuiltInFunctionDefinitions.class.getDeclaredFields();

    for (Field field : fields) {
      // Check if field type is SqlFunction
      if (BuiltInFunctionDefinition.class.isAssignableFrom(field.getType())) {
        if (toExclude.contains(field.getName())) {
          continue;
        }
        functions.add(createFunctionFromFlink(field.getName()));
      }
    }

    return functions;
  }

  public NamePath getPath() {
    return LIB_NAME;
  }

}
