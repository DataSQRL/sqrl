/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.functions.time;

import static com.datasqrl.NamespaceObjectUtil.createFunctionFromStdOpTable;
import static com.datasqrl.time.TimeFunctions.AT_ZONE;
import static com.datasqrl.time.TimeFunctions.END_OF_DAY;
import static com.datasqrl.time.TimeFunctions.END_OF_HOUR;
import static com.datasqrl.time.TimeFunctions.END_OF_MINUTE;
import static com.datasqrl.time.TimeFunctions.END_OF_MONTH;
import static com.datasqrl.time.TimeFunctions.END_OF_SECOND;
import static com.datasqrl.time.TimeFunctions.END_OF_WEEK;
import static com.datasqrl.time.TimeFunctions.END_OF_YEAR;
import static com.datasqrl.time.TimeFunctions.EPOCH_MILLI_TO_TIMESTAMP;
import static com.datasqrl.time.TimeFunctions.EPOCH_TO_TIMESTAMP;
import static com.datasqrl.time.TimeFunctions.STRING_TO_TIMESTAMP;
import static com.datasqrl.time.TimeFunctions.TIMESTAMP_TO_EPOCH;
import static com.datasqrl.time.TimeFunctions.TIMESTAMP_TO_EPOCH_MILLI;
import static com.datasqrl.time.TimeFunctions.TIMESTAMP_TO_STRING;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.AbstractFunctionModule;
import com.datasqrl.function.StdLibrary;
import com.datasqrl.module.NamespaceObject;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.table.functions.FunctionDefinition;

@AutoService(StdLibrary.class)
public class StdTimeLibraryImpl extends AbstractFunctionModule implements StdLibrary {
  public static final NamePath LIB_NAME = NamePath.of("time");
  public static final List<FunctionDefinition> SQRL_FUNCTIONS = List.of(
      EPOCH_TO_TIMESTAMP,
      EPOCH_MILLI_TO_TIMESTAMP,
      TIMESTAMP_TO_EPOCH,
      TIMESTAMP_TO_EPOCH_MILLI,
      STRING_TO_TIMESTAMP,
      TIMESTAMP_TO_STRING,
      AT_ZONE
  );

  public static final StdTimeLibraryImpl stdTimeLibrary = new StdTimeLibraryImpl();

  public StdTimeLibraryImpl() {
    super(SQRL_FUNCTIONS.stream().map(NamespaceObjectUtil::createNsObject).collect(Collectors.toList()));
  }

  public NamePath getPath() {
    return LIB_NAME;
  }

}
