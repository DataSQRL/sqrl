package com.datasqrl.functions.json;

import com.datasqrl.json.JsonFunctions;
import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.AbstractFunctionModule;
import com.datasqrl.function.StdLibrary;
import com.datasqrl.module.NamespaceObject;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.functions.FunctionDefinition;

@AutoService(StdLibrary.class)
public class StdJsonLibraryImpl extends AbstractFunctionModule implements StdLibrary {

  public static final List<FunctionDefinition> JSON_FUNCTIONS = List.of(
      JsonFunctions.TO_JSON,
      JsonFunctions.JSON_TO_STRING,
      JsonFunctions.JSON_OBJECT,
      JsonFunctions.JSON_ARRAY,
      JsonFunctions.JSON_EXTRACT,
      JsonFunctions.JSON_QUERY,
      JsonFunctions.JSON_EXISTS,
      JsonFunctions.JSON_CONCAT,
      JsonFunctions.JSON_ARRAYAGG,
      JsonFunctions.JSON_OBJECTAGG
  );
  private static final NamePath LIB_NAME = NamePath.of("json");

  public StdJsonLibraryImpl() {
    super(JSON_FUNCTIONS.stream().map(NamespaceObjectUtil::createNsObject).collect(Collectors.toList()));
  }

  public NamePath getPath() {
    return LIB_NAME;
  }
}
