/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

import static com.datasqrl.NamespaceObjectUtil.createFunctionFromFlink;
import static com.datasqrl.TimeFunctions.AT_ZONE;
import static com.datasqrl.TimeFunctions.END_OF_DAY;
import static com.datasqrl.TimeFunctions.END_OF_HOUR;
import static com.datasqrl.TimeFunctions.END_OF_INTERVAL;
import static com.datasqrl.TimeFunctions.END_OF_MINUTE;
import static com.datasqrl.TimeFunctions.END_OF_MONTH;
import static com.datasqrl.TimeFunctions.END_OF_SECOND;
import static com.datasqrl.TimeFunctions.END_OF_WEEK;
import static com.datasqrl.TimeFunctions.END_OF_YEAR;
import static com.datasqrl.TimeFunctions.EPOCH_MILLI_TO_TIMESTAMP;
import static com.datasqrl.TimeFunctions.EPOCH_TO_TIMESTAMP;
import static com.datasqrl.TimeFunctions.NOW;
import static com.datasqrl.TimeFunctions.STRING_TO_TIMESTAMP;
import static com.datasqrl.TimeFunctions.TIMESTAMP_TO_EPOCH;
import static com.datasqrl.TimeFunctions.TIMESTAMP_TO_STRING;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.FunctionNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlOperator;
import org.apache.commons.collections.ListUtils;

@AutoService(StdLibrary.class)
public class StdTimeLibraryImpl extends AbstractFunctionModule implements StdLibrary {
  public static final NamePath LIB_NAME = NamePath.of("time");
  public static final List<SqrlFunction> SQRL_FUNCTIONS = List.of(
//      NOW,
      EPOCH_TO_TIMESTAMP,
      EPOCH_MILLI_TO_TIMESTAMP,
      TIMESTAMP_TO_EPOCH,
      STRING_TO_TIMESTAMP,
      TIMESTAMP_TO_STRING,
      AT_ZONE,
      END_OF_SECOND,
      END_OF_MINUTE,
      END_OF_HOUR,
      END_OF_DAY,
      END_OF_WEEK,
      END_OF_MONTH,
      END_OF_YEAR,
      END_OF_INTERVAL
  );

  private static List<NamespaceObject> SQL_FUNCTIONS = List.of(
      createFunctionFromFlink("second"),
      createFunctionFromFlink("minute"),
      createFunctionFromFlink("hour"),
      createFunctionFromFlink("dayOfWeek"),
      createFunctionFromFlink("dayOfMonth"),
      createFunctionFromFlink("dayOfYear"),
      createFunctionFromFlink("month"),
      createFunctionFromFlink("week"),
      createFunctionFromFlink("quarter"),
      createFunctionFromFlink("year")
  );

  public static final StdTimeLibraryImpl stdTimeLibrary = new StdTimeLibraryImpl();

  public StdTimeLibraryImpl() {
    super(ListUtils.union(SQRL_FUNCTIONS.stream().map(NamespaceObjectUtil::createNsObject).collect(Collectors.toList()),SQL_FUNCTIONS));
  }

  public NamePath getPath() {
    return LIB_NAME;
  }
  /* ========
      Function Class Implementations
     ========
   */
  public static Optional<SqrlFunction> lookupSQRLFunction(SqlOperator operator) {
    if (operator.getName().equalsIgnoreCase("now")) {
      return Optional.of(NOW);
    }
    //lookup time fnc
    Optional<NamespaceObject> ns = stdTimeLibrary.getNamespaceObject(Name.system(operator.getName()));
    if (ns.isPresent()) {
      return ns.filter(n->n instanceof FunctionNamespaceObject)
          .map(n->((FunctionNamespaceObject)n).getFunction())
          .filter(f->f instanceof SqrlFunction)
          .map(f->(SqrlFunction)f);
    }

    if (operator instanceof SqrlFunction) {
      return Optional.of((SqrlFunction)operator);
    }
    return Optional.empty();
  }
}
