/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.functions.text;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.table.functions.FunctionDefinition;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.AbstractFunctionModule;
import com.datasqrl.function.CommonFunctions;
import com.datasqrl.function.StdLibrary;
import com.datasqrl.function.text.*;
import com.google.auto.service.AutoService;


@AutoService(StdLibrary.class)
public class StdSqrlLibraryImpl extends AbstractFunctionModule implements StdLibrary {
  public static final NamePath LIB_NAME = NamePath.of("text");
  public static final List<FunctionDefinition> SQRL_FUNCTIONS = List.of(
      new Split(), //TODO: remove for Flink 1.20, since it's part of the standard library
      new TextSearch(),
      new Format(),
      CommonFunctions.SERIALIZE_TO_BYTES,
      CommonFunctions.NOOP,
      CommonFunctions.HASH_COLUMNS);

  public StdSqrlLibraryImpl() {
    super(SQRL_FUNCTIONS.stream().map(NamespaceObjectUtil::createNsObject).collect(Collectors.toList()));
  }

  @Override
public NamePath getPath() {
    return LIB_NAME;
  }

}
