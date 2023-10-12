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
      FlinkSqlOperatorTable.NOW.getClass().getName() //use our NOW
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
