/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.functions.text;

import com.datasqrl.NamespaceObjectUtil;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.AbstractFunctionModule;
import com.datasqrl.function.CommonFunctions;
import com.datasqrl.function.StdLibrary;
import com.datasqrl.text.TextFunctions;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.functions.FunctionDefinition;

@AutoService(StdLibrary.class)
public class StdSqrlLibraryImpl extends AbstractFunctionModule implements StdLibrary {
  public static final NamePath LIB_NAME = NamePath.of("text");
  public static final List<FunctionDefinition> SQRL_FUNCTIONS = List.of(
      TextFunctions.SPLIT, //TODO: remove for Flink 1.20, since it's part of the standard library
      TextFunctions.TEXT_SEARCH,
      TextFunctions.FORMAT,
      CommonFunctions.SERIALIZE_TO_BYTES);

  public StdSqrlLibraryImpl() {
    super(SQRL_FUNCTIONS.stream().map(NamespaceObjectUtil::createNsObject).collect(Collectors.toList()));
  }

  public NamePath getPath() {
    return LIB_NAME;
  }

}
