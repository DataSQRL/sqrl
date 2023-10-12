/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.function;

import static com.datasqrl.NamespaceObjectUtil.createFunctionFromFlink;
import static com.datasqrl.NamespaceObjectUtil.createFunctionFromStdOpTable;
import static com.datasqrl.TimeFunctions.AT_ZONE;
import static com.datasqrl.TimeFunctions.END_OF_DAY;
import static com.datasqrl.TimeFunctions.END_OF_HOUR;
import static com.datasqrl.TimeFunctions.END_OF_MINUTE;
import static com.datasqrl.TimeFunctions.END_OF_MONTH;
import static com.datasqrl.TimeFunctions.END_OF_SECOND;
import static com.datasqrl.TimeFunctions.END_OF_WEEK;
import static com.datasqrl.TimeFunctions.END_OF_YEAR;
import static com.datasqrl.TimeFunctions.EPOCH_MILLI_TO_TIMESTAMP;
import static com.datasqrl.TimeFunctions.EPOCH_TO_TIMESTAMP;
import static com.datasqrl.TimeFunctions.STRING_TO_TIMESTAMP;
import static com.datasqrl.TimeFunctions.TIMESTAMP_TO_EPOCH;
import static com.datasqrl.TimeFunctions.TIMESTAMP_TO_STRING;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.NamespaceObject;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.collections.ListUtils;

@AutoService(StdLibrary.class)
public class StdTimeLibraryImpl extends AbstractFunctionModule implements StdLibrary {
  public static final NamePath LIB_NAME = NamePath.of("time");
  public static final List<SqrlFunction> SQRL_FUNCTIONS = List.of(
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
      END_OF_YEAR
  );

  private static List<NamespaceObject> SQL_FUNCTIONS = List.of(
      createFunctionFromStdOpTable("second"),
      createFunctionFromStdOpTable("minute"),
      createFunctionFromStdOpTable("hour"),
      createFunctionFromStdOpTable("dayOfWeek"),
      createFunctionFromStdOpTable("dayOfMonth"),
      createFunctionFromStdOpTable("dayOfYear"),
      createFunctionFromStdOpTable("month"),
      createFunctionFromStdOpTable("week"),
      createFunctionFromStdOpTable("quarter"),
      createFunctionFromStdOpTable("year")
  );

  public static final StdTimeLibraryImpl stdTimeLibrary = new StdTimeLibraryImpl();

  public StdTimeLibraryImpl() {
    super(ListUtils.union(SQRL_FUNCTIONS.stream().map(NamespaceObjectUtil::createNsObject).collect(Collectors.toList()),SQL_FUNCTIONS));
  }

  public NamePath getPath() {
    return LIB_NAME;
  }

}
